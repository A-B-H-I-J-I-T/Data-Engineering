import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import argparse
from jsonschema import  Draft7Validator, FormatChecker,ValidationError
from select_main_image import GetNewMainImage

# read the jsonl file
class ReadJSONLFile(beam.DoFn):
    def process(self, element):
        with open(element, 'r') as f:
            for line in f:
                yield json.loads(line)

# validate the schema of input jsonl files
class ValidateSchema(beam.DoFn):
    def __init__(self, schema):
        self.schema = schema
        self.validator = Draft7Validator(schema, format_checker=FormatChecker())
    

    def process(self, element):
        if self.validator.is_valid(element):
            yield (element, "Valid")
        else:
            errors = sorted(self.validator.iter_errors(element), key=lambda e: e.path)
            errors = errors[0].message
            yield (element, "Invalid", errors)

#read schema file
def ReadSchema(s_path):
    with open(s_path, 'r') as file:
        schema = json.load(file)
    return schema

# get valid images, tags and main image
def GetValidRecords(p, files, schema, name):
    files = p | f'Create file list {name}' >> beam.Create(files)
    
    files_jsonl = (files
                    | f'Read JSONL files {name}' >> beam.ParDo(ReadJSONLFile())
                    )
  
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

#func join the images and tags 
def merge_image_n_tags(kv_pair):
    left, right = kv_pair[1]['left'], kv_pair[1]['right']
    if left and right:  # Ensure both sides have data
        merged_record = {**left[0], **right[0]}  
        return merged_record

    elif left and not right:
        merged_record = {**left[0], **{'tags':[]}}
        return merged_record

def create_kv_pair(element):
    if 'value' in element.keys():
        key = (element['key']['hotel_id'], element['value']['image_id'])
    else:
        key = (element['key']['hotel_id'], None)
    return (key, element)

#func to calculate the cdc
def changeDataCapture(grpd_hotels):
    if not bool(grpd_hotels[1]['left']) and bool(grpd_hotels[1]['right']):
        return ('newly', grpd_hotels[1]['right'][0] )
    elif not bool(grpd_hotels[1]['right']) and bool(grpd_hotels[1]['left']):
        return_deleted = grpd_hotels[1]['left'][0]
        return_deleted['value'] = {}
        return ('deleted', return_deleted )
    elif  bool(grpd_hotels[1]['left']) and bool(grpd_hotels[1]['right']):
        if grpd_hotels[1]['left'][0]['value']['image_id'] != grpd_hotels[1]['right'][0]['value']['image_id']:
                return ('updated', grpd_hotels[1]['right'][0])

# func to calculate metrics
def calculateMetrics(grpd_hotels):
    if not bool(grpd_hotels[1]['left']) and bool(grpd_hotels[1]['right']):
        return 'newly'
    elif not bool(grpd_hotels[1]['right']) and bool(grpd_hotels[1]['left']):
        return 'deleted'
    elif  bool(grpd_hotels[1]['left']) and bool(grpd_hotels[1]['right']):
        if grpd_hotels[1]['left'][0]['value']['image_id'] != grpd_hotels[1]['right'][0]['value']['image_id']:
                return 'updated'

#func to agg metrics
def metrics_agg_format(metrics_agg):
    merged_dict = { }
    for d in metrics_agg:
        merged_dict.update(d)
    return merged_dict

#func to format the metrics result
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

# func to add the invalidation reason for invalid records
def addInvalidationReason(invalid_records):
    invalid_dict = invalid_records[0]
    invalid_dict.update({'Invalidation Reason':invalid_records[1]})
    return invalid_dict

#main run method
def run():
    #take arguments from CLI
    parser = argparse.ArgumentParser(description='Select and aggregate metrics for main images')
    parser.add_argument('--images', type=str, required=True,default = './data/images.jsonl', help='Local filesystem path to the JSONL file containing the images.')
    parser.add_argument('--tags', type=str, required=True, default='./data/image_tags.jsonl', help='Local filesystem path to the JSONL file containing the image tags.')
    parser.add_argument('--main_images', type=str, required=True,default='./data/main_images.jsonl', help='Local filesystem path to the JSONL file containing the existing main images.')
    parser.add_argument('--output_cdc', type=str, required=True,default='./cdc/cdc', help='Local filesystem path to write JSONL file(s) containing the changes.')
    parser.add_argument('--output_snapshot', type=str, required=True,default='./snapshot/snapshot', help='Local filesystem path to write JSONL file(s) containing the snapshot of the pipeline run.')
    parser.add_argument('--output_metrics', type=str, required=True,default='./metrics/metrics', help='Local filesystem path to a single JSONL file to write the metrics of the pipeline.')
    args = parser.parse_args()

    #get file path of the input files
    image_tags = [args.tags]  # List of your JSONL files
    images = [args.images] 
    main_images = [args.main_images] 

    # get all the schema files
    tags_schema = ReadSchema('./schemas/image_tags.json')
    image_schema = ReadSchema('./schemas/image.json')
    main_schema = ReadSchema('./schemas/main_image.json')
    
    #pipeline
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        #validate image_tags files
        tags_valid_records, tags_invalid_records = GetValidRecords(p, image_tags, tags_schema, "tags")
        tags_valid_records = (tags_valid_records 
                              | 'Tags to tuple' >> beam.Map(lambda x:( x['image_id'],x))
                                 )

        #validate images files
        image_valid_records, image_invalid_records = GetValidRecords(p, images, image_schema, "images")
        image_valid_records = (image_valid_records
                                | 'Image to tuple' >> beam.Map(lambda x: (x['image_id'],x))   
                                                )
        #validate main images files
        main_valid_records, main_invalid_records = GetValidRecords(p, main_images, main_schema, "main")
        main_valid_records = (main_valid_records
                            | 'old_main_image to tuple'  >> beam.Map( lambda x:(x['key']['hotel_id'],x))
        )
        
        # write all invalid records
        (tags_invalid_records 
         | 'Map the invalid tag message' >> beam.Map(addInvalidationReason)
         |'Write invalid tags' >> beam.io.WriteToText('./invalids/invalid_tags', shard_name_template='', file_name_suffix='.jsonl')
        )
        
        (image_invalid_records 
         | 'Map the invalid image message' >> beam.Map(addInvalidationReason)
         |'Write invalid images' >> beam.io.WriteToText('./invalids/invalid_images', shard_name_template='', file_name_suffix='.jsonl')
        )
        
        (main_invalid_records 
         | 'Map the invalid main message' >> beam.Map(addInvalidationReason)
         |'Write invalid main images' >> beam.io.WriteToText('./invalids/invalid_main_images', shard_name_template='', file_name_suffix='.jsonl')
        )

        # join image and tags
        image_with_tags = ({'left':image_valid_records,'right':tags_valid_records}
                          | 'Group by Image' >> beam.CoGroupByKey()
                          | 'Filter and Merge' >> beam.Map(merge_image_n_tags)
                          | 'Remove None' >> beam.Filter(lambda x: x is not None)
                          | 'Set to group by Hotel' >> beam.Map(lambda x : (x['hotel_id'],x))
                           )
        # group by hotels
        Images_gb_hotel = (image_with_tags
                           | 'Group by hotel' >> beam.GroupByKey()                
        )
        #Calculate new main image
        main_images_snapshot = (Images_gb_hotel
                          |  'Get main image' >> beam.ParDo(GetNewMainImage())
                          | 'new_main_image to tuple' >> beam.Map( lambda x:(x['key']['hotel_id'],x))
        )

        #Caculate the CDC and write it
        cdc = ({'left':main_valid_records,'right':main_images_snapshot}
                          | 'Group by hotels and image' >> beam.CoGroupByKey()
                          | 'Create the delta keys' >> beam.Map(changeDataCapture)
                          | 'CDC ' >> beam.Map(lambda x: x [1] )
                          | 'Write CDC' >> beam.io.WriteToText(args.output_cdc, shard_name_template='', file_name_suffix='.jsonl')
        )

        #Calculate count of new, updated and deleted main images
        metrics = ({'left':main_valid_records,'right':main_images_snapshot}
                          | 'Group by hotels and image for metrics' >> beam.CoGroupByKey()
                          | 'Create the delta keys for metrics' >> beam.Map(calculateMetrics)
                          | 'Calculate count' >> beam.combiners.Count.PerElement()
        )
        main_images_snapshot | 'Write snapshot' >> beam.io.WriteToText(args.output_snapshot, shard_name_template='', file_name_suffix='.jsonl')

        # find number of images
        no_of_images = (image_valid_records
                                | 'No of Images' >> beam.combiners.Count.Globally()
                                | 'Extract Image Id' >> beam.Map(lambda x: ('no_of_img_prc',x))
        )
        # find the number of hotels with images
        no_of_h_w_images = (image_valid_records
                                | 'Get Hotels' >> beam.Map(lambda x: x[1]['hotel_id'])
                                |'Distinct Hotels'>> beam.Distinct()
                                | 'No of Hotels' >> beam.combiners.Count.Globally()
                                | 'Extract Hotels Id' >> beam.Map(lambda x: ('no_of_hotel_prc',x))

        )
        # Aggregate and format the metrics 
        metrics_agg = ((metrics, no_of_images, no_of_h_w_images) 
                       | 'Merge PCollections' >> beam.Flatten()
                       | 'CollectTuples' >> beam.Map(lambda t: {t[0]:t[1]})
                       | 'Combine all dict' >> beam.CombineGlobally(metrics_agg_format)
                       | 'Format the metrics jsonl' >> beam.Map(format_metrics)
                       | 'Write metrics' >> beam.io.WriteToText(args.output_metrics, shard_name_template='', file_name_suffix='.jsonl')
                       )
        
if __name__ == "__main__":
    run()