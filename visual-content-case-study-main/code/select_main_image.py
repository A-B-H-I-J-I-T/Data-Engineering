import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
from jsonschema import validate, ValidationError
# from jsonschema import  Draft7Validator, FormatChecker,ValidationError

class ReadJSONLFile(beam.DoFn):
    def process(self, element):
        with open(element, 'r') as f:
            for line in f:
                yield json.loads(line)

class ValidateSchema(beam.DoFn):
    def __init__(self, schema):
        self.schema = schema

    def process(self, element):
        try:
            validate(instance=element, schema=self.schema)
            yield (element, "Valid")
        except ValidationError as e:
            yield (element, f"Invalid: {e.message}")

def run():
    image_tags = ['./data/image_tags.jsonl']  # List of your JSONL files
    schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
            "email": {"type": "string", "format": "email"}
        },
        "required": ["name", "age", "email"]
    }
    
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        image_tags = p | 'Create file list' >> beam.Create(image_tags)
        
        image_tags_jsonl = (image_tags
                     | 'Read JSONL files' >> beam.ParDo(ReadJSONLFile())
                     )
        image_tags_jsonl | 'Print records' >> beam.Map(print)       
        # validation_result = (json_data
        #                      | 'Validate schema' >> beam.ParDo(ValidateSchema(schema))
        #                      )
        
        # valid_records = (validation_result
        #                  | 'Filter valid records' >> beam.Filter(lambda x: x[1] == "Valid")
        #                  | 'Get valid records' >> beam.Map(lambda x: x[0])
        #                  )

        # invalid_records = (validation_result
        #                    | 'Filter invalid records' >> beam.Filter(lambda x: not x[1] == "Valid")
        #                    | 'Get invalid records' >> beam.Map(lambda x: (x[0], x[1]))
        #                    )
        
        # valid_records | 'Print valid records' >> beam.Map(print)
        # invalid_records | 'Print invalid records' >> beam.Map(print)

if __name__ == "__main__":
    run()