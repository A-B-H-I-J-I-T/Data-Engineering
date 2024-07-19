import apache_beam as beam

pipeline = beam.Pipeline()

# Example pipeline code
lines = pipeline | beam.Create(['Hello', 'World'])
lines | beam.io.WriteToText('output.txt')

pipeline.run()
