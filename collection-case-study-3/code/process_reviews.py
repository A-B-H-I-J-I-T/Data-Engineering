import argparse
import json
import pandas as pd
from jsonschema import  Draft7Validator, FormatChecker,ValidationError
import numpy as np
import re
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta

 

def read_jsonl_file(file_path):
    data = []
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            data.append(json.loads(line.strip()))
    return data

def write_jsonl_file(data, file_path):
    with open(file_path, 'w', encoding='utf-8') as file:
        for item in data:
            file.write(json.dumps(item) + '\n')

def read_schema_file(file_path):
    with open(file_path, 'r') as file:
        schema = json.load(file)

    return schema

def validate_schema(data, schema):
    valid_data = []
    invalid_data = []
    validator = Draft7Validator(schema, format_checker=FormatChecker())
    for record in data:
        if validator.is_valid(record):
            valid_data.append(record)
        else:
            invalid_data.append(record)
    return valid_data
##For this specific task I chose numpy instead of pandas as numpy are faster and we are processing each row
def replace_foul_words(np_review, np_foul):
    for a in np_review:
        review = a[2]
        lst_review = review.split()
        lst_review = [re.sub(r'\W+', '', word.lower()) for word in lst_review ]
        total_words = len(lst_review)# get total count of word
        inappropriate_count = 0
        ##replace the foul word with **** and calculate the percentage
        for inap_word in np_foul:
            pattern = re.compile(re.escape(inap_word), re.IGNORECASE)
            review, count = pattern.subn("****", review)
            inappropriate_count += count
        percentage = (inappropriate_count / total_words)  if total_words > 0 else 0
        ## replace the numpy values
        a[2] = review
        a[-1] = percentage
    return np_review
 
## Here pandas are better to work with as it is column based operation 
def aggregate_values(df_fil_review):
    ## find review age in days
    today = pd.Timestamp.now(tz='UTC').normalize()
    df_fil_review['reviewAge'] = df_fil_review['publishedAt'].dt.normalize()
    df_fil_review['reviewAge'] = (today - df_fil_review['reviewAge']).dt.days
    ## define aggreagation
    agg_funcs = {
        'reviewId':'count',
        'rating':  'mean',
        'averageReviewLength': 'mean',
        'reviewAge': ['max', 'min', 'mean']
    }
    ##apply agg
    df_agg_review = df_fil_review.groupby('restaurantId').agg(agg_funcs)
    df_agg_review = df_agg_review.reset_index()
    ## rename the columns
    df_agg_review.columns = ['restaurantId','reviewCount','averageRating','averageReviewLength',
                               'reviewAge_oldest',
                                'reviewAge_newest',
                                'reviewAge_average']

    return df_agg_review
## after finding the aggregate values lets convert to the write json format
def get_agg_jsonl_file(df_agg_review):
    agg_json = []
    for index,row in df_agg_review.iterrows():
        json_dict = {
            'restaurantId': int(row['restaurantId']),
            'reviewCount' : int(row['reviewCount']),
            'averageRating' : float(row['averageRating']),
            'averageReviewLength' : int(row['averageReviewLength']),
            'reviewAge':{"oldest": int(row['reviewAge_oldest']),
                        "newest":int(row['reviewAge_newest']),
                        "average":int(row['reviewAge_average'])
                        }
        }
        json_str = json_dict
        agg_json.append(json_str)

    return agg_json

def main():
    ## get the values from terminal
    parser = argparse.ArgumentParser(description='Filter and aggregate reviews from JSONL file.')
    parser.add_argument('--input', type=str, required=True, help='Path to input JSONL file containing reviews')
    parser.add_argument('--inappropriate_words', type=str, required=True, help='Path to text file with inappropriate words')
    parser.add_argument('--output', type=str, required=True, help='Path to output JSONL file for filtered reviews')
    parser.add_argument('--aggregations', type=str, required=True, help='Path to output JSONL file for aggregations')
    args = parser.parse_args()

    #load reviews and schema file
    reviews = read_jsonl_file(args.input)
    
    schema = read_schema_file('schemas/review.json')

    # Validate input reviews
    valid_data = validate_schema(reviews, schema)

    dtype = [('restaurantId', 'i4'),  
            ('reviewId', 'i4'), 
            ('text', object), 
            ('rating', 'f4'),   
            ('publishedAt', 'U24'), 
            ('percentage', 'f4')]   
    # Create an empty structured np array
    np_review = np.zeros(len(valid_data), dtype=dtype)
    # create a np array because it is faster than pandas and good fo row processing
    for i, item in enumerate(valid_data):
        # print(item)
        np_review[i] = (item['restaurantId'], item['reviewId'], item['text'],item['rating'],item['publishedAt'],0)


    # load inappropriate words

    np_inapt_word = np.genfromtxt(args.inappropriate_words, 
                                  dtype='str', encoding='utf-8')

    #replace inappropriate words
    filtered_review = replace_foul_words(np_review, np_inapt_word)

    # convert to pandas df good for column based aggregations
    df_fil_review = pd.DataFrame(filtered_review)
    df_fil_review['publishedAt'] = pd.to_datetime(df_fil_review['publishedAt'],format='mixed', utc=True)

    # Define the data types explicitly
    df_fil_review = df_fil_review.astype({
        'restaurantId': 'int32',
        'reviewId': 'int32',
        'text': 'string',
        'rating': 'float32',
        'publishedAt': 'datetime64[ns, UTC]',
        'percentage': 'float32'
    })
    # Get today's date
    today = pd.Timestamp.now(tz='UTC').date()

    # Calculate the date three years ago from today using relativedelta
    three_years_ago = today - relativedelta(years=3)

    # filter review oler than 3 years abd fould word percenatage >= 20%
    df_fil_review = df_fil_review[(df_fil_review['publishedAt'].dt.date>=three_years_ago)&
                                  (df_fil_review['percentage']<=0.2)]
    
    df_fil_review = df_fil_review.drop(columns=['percentage'])

    # remove duplicate reviews
    df_grouped = df_fil_review.groupby(['restaurantId','reviewId'])
    # take latest processed review
    idx_max_publish_dt = df_grouped['publishedAt'].idxmax()
    df_fil_review = df_fil_review.loc[idx_max_publish_dt]

    # write the processed review
    df_fil_review.to_json(args.output, orient='records', lines=True, date_format='iso')
    #get the char length for each review
    df_fil_review['averageReviewLength'] = df_fil_review['text'].apply(len)
    #drop the column text as we don't need it for aggreagation
    df_fil_review = df_fil_review.drop(columns=['text'])

    #aggregate the values using pandas
    df_agg_review = aggregate_values(df_fil_review)

    # get the json format to write the aggregate values
    agg_json = get_agg_jsonl_file(df_agg_review)

    # check data quality
    # read schema file
    agg_schema = read_schema_file('schemas/aggregation.json')
    # validate the schema and also can set alerts for entries that breaches it
    agg_json_valid = validate_schema(agg_json,agg_schema)

    # write the aggregated json file.
    write_jsonl_file(agg_json_valid,args.aggregations)


if __name__ == '__main__':
    main()
