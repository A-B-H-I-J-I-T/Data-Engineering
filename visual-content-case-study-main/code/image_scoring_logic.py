import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import math
from datetime import datetime

# Constants
MIN_RES = 160000
MAX_RES = 2073600

MIN_AR = 0.3
MAX_AR = 4.65

MAX_FRESHNESS_DAY = 10 * 365  # 10 years

WEIGHT_RESOLUTION = 6
WEIGHT_ASPECT_RATIO = 2
WEIGHT_FRESHNESS = 2
WEIGHT_TAG_PRIORITY = 3

# Helper Functions
def compute_resolution_score(resolution):
    if resolution <= 0:
        return 0
    score = (math.log(resolution) - math.log(MIN_RES)) / (math.log(MAX_RES) - math.log(MIN_RES))
    return min(score, 1)

def compute_aspect_ratio_score(ar):
    if ar is None or ar < MIN_AR or ar > MAX_AR:
        return 0
    return 1  # Implement specific AR scoring rules if needed

def compute_freshness_score(created_date):
    today = datetime.today().date()
    created_date = datetime.strptime(created_date, "%Y-%m-%d").date()
    freshness = (today - created_date).days
    score = 1 + (-1 * (freshness / MAX_FRESHNESS_DAY))
    return max(0, min(score, 1))

def compute_tag_priority_score(tags):
    # Placeholder - implement specific tag priority scoring rules
    return 1 if tags else 0

def compute_image_score(image):
    width = image['width']
    height = image['height']
    created_date = image['created_date']
    tags = image['tags']
    
    resolution = width * height
    ar = None if height == 0 else width / height
    
    score_res = compute_resolution_score(resolution)
    score_ar = compute_aspect_ratio_score(ar)
    score_fresh = compute_freshness_score(created_date)
    score_tag = compute_tag_priority_score(tags)
    
    score_image = (
        WEIGHT_RESOLUTION * score_res +
        WEIGHT_ASPECT_RATIO * score_ar +
        WEIGHT_FRESHNESS * score_fresh +
        WEIGHT_TAG_PRIORITY * score_tag
    ) / (
        WEIGHT_RESOLUTION + WEIGHT_ASPECT_RATIO +
        WEIGHT_FRESHNESS + WEIGHT_TAG_PRIORITY
    )
    
    return score_image

class ExtractImages(beam.DoFn):
    def process(self, element):
        item = json.loads(element)
        item_id = item['item_id']
        for image in item['images']:
            image['item_id'] = item_id
            yield image

class AssignHighScore(beam.DoFn):
    def process(self, element):
        item_id, images = element
        highest_score = -1
        main_image = None
        for image in images:
            score = compute_image_score(image)
            if score > highest_score:
                highest_score = score
                main_image = image
        
        if main_image:
            main_image['main_image_score'] = highest_score
            yield main_image

# Pipeline Setup
def run(argv=None):
    options = PipelineOptions(argv)
    
    with beam.Pipeline(options=options) as p:
        (p 
         | 'ReadInput' >> beam.io.ReadFromText('input.jsonl')
         | 'ExtractImages' >> beam.ParDo(ExtractImages())
         | 'GroupByItem' >> beam.GroupByKey()
         | 'AssignHighScore' >> beam.ParDo(AssignHighScore())
         | 'WriteOutput' >> beam.io.WriteToText('output.jsonl')
        )

if __name__ == '__main__':
    run()