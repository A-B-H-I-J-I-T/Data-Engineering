import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import argparse
# from jsonschema import validate, ValidationError
from jsonschema import  Draft7Validator, FormatChecker,ValidationError
from select_main_image import GetNewMainImage

class ReadJSONLFile(beam.DoFn):
    def process(self, element):
        with open(element, 'r') as f:
            for line in f:
                yield json.loads(line)

class ValidateSchema(beam.DoFn):
    def __init__(self, schema):
        self.schema = schema
        self.validator = Draft7Validator(schema, format_checker=FormatChecker())
    # Validate JSON data against the schema
    

    def process(self, element):
        # errors = sorted(self.validator.iter_errors(element), key=lambda e: e.path)
        # print([error.message for error in errors])
        if self.validator.is_valid(element):
            yield (element, "Valid")
        else:
            errors = sorted(self.validator.iter_errors(element), key=lambda e: e.path)
            errors = errors[0].message
            # errors = [error.message for error in errors]
            yield (element, "Invalid", errors)
        
        # try:
        #     validate(instance=element, schema=self.schema)
        #     yield (element, "Valid")
        # except ValidationError as e:
        #     yield (element, f"Invalid: {e.message}")

#read schema file
def ReadSchema(s_path):
    with open(s_path, 'r') as file:
        schema = json.load(file)
    return schema

def GetValidRecords(p, files, schema, name):
    files = p | f'Create file list {name}' >> beam.Create(files)
    
    files_jsonl = (files
                    | f'Read JSONL files {name}' >> beam.ParDo(ReadJSONLFile())
                    )

    # files_jsonl | f'Print records {name}' >> beam.Map(print)       
    validation_result = (files_jsonl
                            | f'Validate schema {name}' >> beam.ParDo(ValidateSchema(schema))
                            )
    
    valid_records = (validation_result
                        | f'Filter valid records {name}' >> beam.Filter(lambda x: x[1] == "Valid")
                        | f'Get valid records {name}' >> beam.Map(lambda x: x[0])
                        )
    
    invalid_records = (validation_result
                        | f'Filter invalid records {name}' >> beam.Filter(lambda x:  x[1] == "Invalid")
                        | f'Get invalid records {name}' >> beam.Map(lambda x: (x[0], x[2]))
                        )
    return valid_records , invalid_records

def merge_image_n_tags(kv_pair):
    """Merges the records for a given key."""
    left, right = kv_pair[1]['left'], kv_pair[1]['right']
    if left and right:  # Ensure both sides have data
        merged_record = {**left[0], **right[0]}  # Merge dictionaries
        return merged_record

    elif left and not right:
        merged_record = {**left[0], **{'tags':[]}}
        return merged_record

# def merge_image_n_tags(kv_pair):
#     """Merges the records for a given key."""
#     left, right = kv_pair[1]['left'], kv_pair[1]['right']
#     if left and right:
#         right = right[0]['tags']
#         if right:  # Ensure both sides have data
#             merged_record = {**left[0], **right[0]}  # Merge dictionaries
#             return merged_record



def create_kv_pair(element):
    if 'value' in element.keys():
        key = (element['key']['hotel_id'], element['value']['image_id'])
    else:
        key = (element['key']['hotel_id'], None)
    return (key, element)

def changeDataCapture(grpd_hotels):
    # errors = sorted(self.validator.iter_errors(element), key=lambda e: e.path)
    # print([error.message for error in errors])
    if not bool(grpd_hotels[1]['left']) and bool(grpd_hotels[1]['right']):
        return ('newly', grpd_hotels[1]['right'][0] )
    elif not bool(grpd_hotels[1]['right']) and bool(grpd_hotels[1]['left']):
        return_deleted = grpd_hotels[1]['left'][0]
        return_deleted['value'] = {}
        return ('deleted', return_deleted )
    elif  bool(grpd_hotels[1]['left']) and bool(grpd_hotels[1]['right']):
        if grpd_hotels[1]['left'][0]['value']['image_id'] != grpd_hotels[1]['right'][0]['value']['image_id']:
                return ('updated', grpd_hotels[1]['right'][0])
        # elif grpd_hotels[1]['left'][0]['value']['image_id'] == grpd_hotels[1]['right'][0]['value']['image_id']:
        #         return ('nochange', grpd_hotels[1]['right'][0] )

def calculateMetrics(grpd_hotels):
    # errors = sorted(self.validator.iter_errors(element), key=lambda e: e.path)
    # print([error.message for error in errors])
    if not bool(grpd_hotels[1]['left']) and bool(grpd_hotels[1]['right']):
        return 'newly'
    elif not bool(grpd_hotels[1]['right']) and bool(grpd_hotels[1]['left']):
        return 'deleted'
    elif  bool(grpd_hotels[1]['left']) and bool(grpd_hotels[1]['right']):
        if grpd_hotels[1]['left'][0]['value']['image_id'] != grpd_hotels[1]['right'][0]['value']['image_id']:
                return 'updated'
        # elif grpd_hotels[1]['left'][0]['value']['image_id'] == grpd_hotels[1]['right'][0]['value']['image_id']:
        #         return 'nochange'


# class ChangeDataCapture(beam.DoFn):
#     def __init__(self):
#         self.newly = 0
#         self.updtd = 0
#         self.deleted = 0

#     def process(self, grpd_hotels):
#         # errors = sorted(self.validator.iter_errors(element), key=lambda e: e.path)
#         # print([error.message for error in errors])
#         if not bool(grpd_hotels[1]['left']) and bool(grpd_hotels[1]['right']):
#             self.newly += 1
#         elif not bool(grpd_hotels[1]['right']) and bool(grpd_hotels[1]['left']):
#             self.deleted += 1
#         elif  bool(grpd_hotels[1]['left']) and bool(grpd_hotels[1]['right']):
#             if grpd_hotels[1]['left'][0]['value']['image_id'] != grpd_hotels[1]['right'][0]['value']['image_id']:
#                  self.updtd += 1

def metrics_agg_format(metrics_agg):
    merged_dict = { }
    for d in metrics_agg:
        merged_dict.update(d)
    return merged_dict
    # print(merged_dict)
    # newly = merged_dict['newly']
    # print(newly)
def format_metrics(merged_dict):
    merged_dict = {
    'Number of images processed': merged_dict['no_of_img_prc'] if 'no_of_img_prc' in merged_dict.keys() else 0 ,
    'Number of hotels with images': merged_dict['no_of_hotel_prc'] if 'no_of_hotel_prc' in merged_dict.keys() else 0,
    'Number of main images': {
        'Newly elected' : merged_dict['newly']if 'newly' in merged_dict.keys() else 0,
        'Updated': merged_dict['updated'] if 'updated' in merged_dict.keys() else 0,
        'Deleted': merged_dict['deleted'] if 'deleted' in merged_dict.keys() else 0

    }
    }

    return merged_dict

def addInvalidationReason(invalid_records):
    invalid_dict = invalid_records[0]
    invalid_dict.update({'Invalidation Reason':invalid_records[1]})
    return invalid_dict

def run():

    parser = argparse.ArgumentParser(description='Select and aggregate metrics for main images')
    parser.add_argument('--images', type=str, required=True,default = './data/images.jsonl', help='Local filesystem path to the JSONL file containing the images.')
    parser.add_argument('--tags', type=str, required=True, default='./data/image_tags.jsonl', help='Local filesystem path to the JSONL file containing the image tags.')
    parser.add_argument('--main_images', type=str, required=True,default='./data/main_images.jsonl', help='Local filesystem path to the JSONL file containing the existing main images.')
    parser.add_argument('--output_cdc', type=str, required=True,default='./cdc/cdc', help='Local filesystem path to write JSONL file(s) containing the changes.')
    parser.add_argument('--output_snapshot', type=str, required=True,default='./snapshot/snapshot', help='Local filesystem path to write JSONL file(s) containing the snapshot of the pipeline run.')
    parser.add_argument('--output_metrics', type=str, required=True,default='./metrics/metrics', help='Local filesystem path to a single JSONL file to write the metrics of the pipeline.')
    args = parser.parse_args()

    image_tags = [args.tags]  # List of your JSONL files
    images = [args.images] 
    main_images = [args.main_images] 
    tags_schema = ReadSchema('./schemas/image_tags.json')
    image_schema = ReadSchema('./schemas/image.json')
    main_schema = ReadSchema('./schemas/main_image.json')
    # print(main_schema)
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        #validate image_tags files
        tags_valid_records, tags_invalid_records = GetValidRecords(p, image_tags, tags_schema, "tags")
        tags_valid_records = (tags_valid_records 
                              | 'Tags to tuple' >> beam.Map(lambda x:( x['image_id'],x))#lambda x: tuple(x.get(k)  for k in list(x.keys())))  
                                 )
                                              #  | 'Print it' >> beam.Map(print))


        #validate images files
        image_valid_records, image_invalid_records = GetValidRecords(p, images, image_schema, "images")
        image_valid_records = (image_valid_records
                                | 'Image to tuple' >> beam.Map(lambda x: (x['image_id'],x)) #tuple(x.get(k)  for k in list(x.keys())))  
                                                )#| 'Print Images' >> beam.Map(print)
        #validate main_image files
        main_valid_records, main_invalid_records = GetValidRecords(p, main_images, main_schema, "main")
        main_valid_records = (main_valid_records
                            | 'old_main_image to tuple'  >> beam.Map( lambda x:(x['key']['hotel_id'],x))
                            #   | 'old_main_image to tuple' >> beam.Map(create_kv_pair)
                            #   | 'old_main to tuple' >> beam.Map(lambda x: tuple(tuple(x['key']['hotel_id'],x['value']['image_id']), x)) 
                            #   | 'print old main image' >> beam.Map(print)
        )
        
        (tags_invalid_records 
         | 'Map the invalid tag message' >> beam.Map(addInvalidationReason)
        #  | 'Print tags' >> beam.Map(print)
         |'Write invalid tags' >> beam.io.WriteToText('./invalids/invalid_tags', shard_name_template='', file_name_suffix='.jsonl')
        )
        
        (image_invalid_records 
         | 'Map the invalid image message' >> beam.Map(addInvalidationReason)
        #  | 'Print tags' >> beam.Map(print)
         |'Write invalid images' >> beam.io.WriteToText('./invalids/invalid_images', shard_name_template='', file_name_suffix='.jsonl')
        )
        
        (main_invalid_records 
         | 'Map the invalid main message' >> beam.Map(addInvalidationReason)
        #  | 'Print tags' >> beam.Map(print)
         |'Write invalid main images' >> beam.io.WriteToText('./invalids/invalid_main_images', shard_name_template='', file_name_suffix='.jsonl')
        )

        image_with_tags = ({'left':image_valid_records,'right':tags_valid_records}
                          | 'Group by Image' >> beam.CoGroupByKey()#lambda x: [{**x[1]['image'][1] ,**x[1]['tags'][0] } if x[1]['image'] and x[1]['tags'] else {} ])
                          | 'Filter and Merge' >> beam.Map(merge_image_n_tags)
                          | 'Remove None' >> beam.Filter(lambda x: x is not None)
                          | 'Set to group by Hotel' >> beam.Map(lambda x : (x['hotel_id'],x))
                           )
        
        Images_gb_hotel = (image_with_tags
                           | 'Group by hotel' >> beam.GroupByKey()                
        )

        main_images_snapshot = (Images_gb_hotel
                          |  'Get main image' >> beam.ParDo(GetNewMainImage())
                          | 'new_main_image to tuple' >> beam.Map( lambda x:(x['key']['hotel_id'],x))
                        #   | 'Write snapshot' >> beam.io.WriteToText('./snapshot/snapshot', shard_name_template='', file_name_suffix='.jsonl')
                        #   | 'new_main_image to tuple' >> beam.Map(create_kv_pair)
                        #   | 'Print Snapshot' >> beam.Map(print)
        )

        cdc = ({'left':main_valid_records,'right':main_images_snapshot}
                          | 'Group by hotels and image' >> beam.CoGroupByKey()
                          | 'Create the delta keys' >> beam.Map(changeDataCapture)
                          | 'CDC ' >> beam.Map(lambda x: x [1] )
                          | 'Write CDC' >> beam.io.WriteToText(args.output_cdc, shard_name_template='', file_name_suffix='.jsonl')
                        #   | 'Calculate CDC' >> beam.combiners.Count.PerElement()
                        #   | 'Calculate CDC' >> beam.ParDo(ChangeDataCapture())
                        #   | 'Print' >> beam.Map(print)
        )


        metrics = ({'left':main_valid_records,'right':main_images_snapshot}
                          | 'Group by hotels and image for metrics' >> beam.CoGroupByKey()
                          | 'Create the delta keys for metrics' >> beam.Map(calculateMetrics)
                          | 'Calculate count' >> beam.combiners.Count.PerElement()
                        #   | 'mertrics ' >> beam.Map(lambda x: x)

                        #   | 'Write metrics' >> beam.io.WriteToText('./metrics/metrics', shard_name_template='', file_name_suffix='.jsonl')
                        #   | 'Calculate CDC' >> beam.combiners.Count.PerElement()
                        #   | 'Calculate CDC' >> beam.ParDo(ChangeDataCapture())
                        #   | 'Print' >> beam.Map(print)
        )
        main_images_snapshot | 'Write snapshot' >> beam.io.WriteToText(args.output_snapshot, shard_name_template='', file_name_suffix='.jsonl')

        no_of_images = (image_valid_records
                                # 
                                | 'No of Images' >> beam.combiners.Count.Globally()
                                | 'Extract Image Id' >> beam.Map(lambda x: ('no_of_img_prc',x))
                                # | 'Print no hotel_w_images' >> beam.Map(print)
        )

        no_of_h_w_images = (image_valid_records
                                | 'Get Hotels' >> beam.Map(lambda x: x[1]['hotel_id'])
                                |'Distinct Hotels'>> beam.Distinct()
                                | 'No of Hotels' >> beam.combiners.Count.Globally()
                                | 'Extract Hotels Id' >> beam.Map(lambda x: ('no_of_hotel_prc',x))

        )

        metrics_agg = ((metrics, no_of_images, no_of_h_w_images) 
                       | 'Merge PCollections' >> beam.Flatten()
                       | 'CollectTuples' >> beam.Map(lambda t: {t[0]:t[1]})
                       | 'Combine all dict' >> beam.CombineGlobally(metrics_agg_format)
                       | 'Format the metrics jsonl' >> beam.Map(format_metrics)
                       | 'Write metrics' >> beam.io.WriteToText(args.output_metrics, shard_name_template='', file_name_suffix='.jsonl')
                    #    | 'Print no ' >> beam.Map(print)
                       )
        
if __name__ == "__main__":
    run()
        # no_hotel_w_images = (main_images_snapshot 
        #                   | 'Count  new of snapshot' >> beam.combiners.Count.Globally()
        #                 #   | 'Print no hotel_w_images' >> beam.Map(print)
        #                   )

        # print(object(hotel_w_images))

        # score_image(image_with_tags)
        # before this you can deduplicate the record join the tags for image scoring
        # image_with_tags = (( {'image':image_valid_records,'tags':tags_valid_records})
        #                    | 'Merge image and tags' >> beam.CoGroupByKey(lambda element: 
        #                                                                  element if element['image']['image_id']==element['tags']['image_id'] else {})
        #                    | beam.Map(print)

        # )


        # print(type(main_valid_records))



        # main_valid_records | beam.io.WriteToText('output.txt')#'Print valid records' >> beam.Map(print)
        
        # image_tags = p | 'Create file list' >> beam.Create(image_tags)
        
        # image_tags_jsonl = (image_tags
        #              | 'Read JSONL files' >> beam.ParDo(ReadJSONLFile())
        #              )
        
        # # image_tags_jsonl | 'Print records' >> beam.Map(print)       
        # tags_validation_result = (image_tags_jsonl
        #                      | 'Validate schema' >> beam.ParDo(ValidateSchema(tags_schema))
        #                      )
        
        # tags_valid_records = (tags_validation_result
        #                  | 'Filter valid records' >> beam.Filter(lambda x: x[1] == "Valid")
        #                  | 'Get valid records' >> beam.Map(lambda x: x[0])
        #                  )

        # invalid_records = (tags_validation_result
        #                    | 'Filter invalid records' >> beam.Filter(lambda x: not x[1] == "Valid")
        #                    | 'Get invalid records' >> beam.Map(lambda x: (x[0], x[1]))
        #                    )
        

        # invalid_records | 'Print invalid records' >> beam.Map(print)

