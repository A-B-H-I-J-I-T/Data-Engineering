import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
# from jsonschema import validate, ValidationError
from jsonschema import  Draft7Validator, FormatChecker,ValidationError
from image_scoring_logic import GetNewMainImage

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
            yield (element, "Invalid")
        
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
    
    # invalid_records = (validation_result
    #                     | f'Filter invalid records {name}' >> beam.Filter(lambda x: not x[1] == "Valid")
    #                     | f'Get invalid records {name}' >> beam.Map(lambda x: (x[0], x[1]))
    #                     )
    return valid_records

def merge_records(kv_pair):
    """Merges the records for a given key."""
    left, right = kv_pair[1]['left'], kv_pair[1]['right']
    if left and right:  # Ensure both sides have data
        merged_record = {**left[0], **right[0]}  # Merge dictionaries
        return merged_record

# def merge_records(kv_pair):
#     """Merges the records for a given key."""
#     left, right = kv_pair[1]['left'], kv_pair[1]['right']
#     if left and right:
#         right = right[0]['tags']
#         if right:  # Ensure both sides have data
#             merged_record = {**left[0], **right[0]}  # Merge dictionaries
#             return merged_record

def run():
    image_tags = ['./data/image_tags.jsonl']  # List of your JSONL files
    images = ['./data/images.jsonl'] 
    main_images = ['./data/main_images.jsonl'] 
    tags_schema = ReadSchema('./schemas/image_tags.json')
    image_schema = ReadSchema('./schemas/image.json')
    main_schema = ReadSchema('./schemas/main_image.json')
    # print(main_schema)
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        #validate image_tags files
        tags_valid_records = GetValidRecords(p, image_tags, tags_schema, "tags")
        tags_valid_records = (tags_valid_records 
                              | 'Tags to tuple' >> beam.Map(lambda x:( x['image_id'],x))#lambda x: tuple(x.get(k)  for k in list(x.keys())))  
                                 )
                                              #  | 'Print it' >> beam.Map(print))
                             
        #validate images files
        image_valid_records = (GetValidRecords(p, images, image_schema, "images")
                                | 'Image to tuple' >> beam.Map(lambda x: (x['image_id'],x)) #tuple(x.get(k)  for k in list(x.keys())))  
                                                )#| 'Print Images' >> beam.Map(print)
        #validate main_image files
        main_valid_records = GetValidRecords(p, main_images, main_schema, "main")


        image_with_tags = ({'left':image_valid_records,'right':tags_valid_records}
                          | 'Group by Image' >> beam.CoGroupByKey()#lambda x: [{**x[1]['image'][1] ,**x[1]['tags'][0] } if x[1]['image'] and x[1]['tags'] else {} ])
                          | 'Filter and Merge' >> beam.Map(merge_records)
                          | 'Remove None' >> beam.Filter(lambda x: x is not None)
                          | 'Set to group by Hotel' >> beam.Map(lambda x : (x['hotel_id'],x))
                           )
        
        Images_gb_hotel = (image_with_tags
                           | 'Group by hotel' >> beam.GroupByKey()
                           
        )

        new_main_image = (Images_gb_hotel
                          |  'Get main image' >> beam.ParDo(GetNewMainImage())
                          | 'Print' >> beam.Map(print)

        )

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

if __name__ == "__main__":
    run()