import argparse
import json
import pandas as pd
from jsonschema import  Draft7Validator, FormatChecker
import numpy as np
import re
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta



def read_jsonl_file(file_path):
    """Reads a JSONL file and returns a list of dictionaries."""
    data = []
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            data.append(json.loads(line.strip()))
    return data

def write_jsonl_file(data, file_path):
    """Writes a list of dictionaries to a JSONL file."""
    with open(file_path, 'w', encoding='utf-8') as file:
        for item in data:
            file.write(json.dumps(item) + '\n')

def validate_schema(data, schema):
    # Collect valid rows
    valid_data = []
    invalid_data = []
    validator = Draft7Validator(schema, format_checker=FormatChecker())
    for record in data:
        if validator.is_valid(record):
            valid_data.append(record)
        else:
            invalid_data.append(record)
    # valid_data = [list(item.values())  for item in valid_data]

    dtype = [('restaurantId', 'i4'),   # Integer
            ('reviewId', 'i4'),   # Integer
            ('text', object),  # String with max length 43
            ('rating', 'f4'),   # Float
            ('publishedAt', 'U24'),  # String with max length 24 (for datetime)
            ('percentage', 'f4')]   # Float
    # Create an empty structured array
    np_data = np.zeros(len(valid_data), dtype=dtype)

    for i, item in enumerate(valid_data):
        # print(item)
        np_data[i] = (item['restaurantId'], item['reviewId'], item['text'],item['rating'],item['publishedAt'],0)


    # invalid_data = np.array(invalid_data)
    # print(type(data))
    return np_data

def replace_foul_words(np_review, np_foul):
    for a in np_review:
        review = a[2]
        review = review.split()
        total_words = len(review)
        inappropriate_count = 0
        for i, word in enumerate(review):
            clean_word = re.sub(r'\W+', '', word.lower())  # Remove punctuation and make lowercase
            if clean_word in np_foul:
                review[i] = '****'
                inappropriate_count += 1
    
        percentage = (inappropriate_count / total_words)  if total_words > 0 else 0
        cleaned_text = ' '.join(review)
        a[2] = cleaned_text
        a[-1] = percentage
    return np_review
    
    return cleaned_text, percentage    
def main():
    #### argument parser
    parser = argparse.ArgumentParser(description='Filter and aggregate reviews from JSONL file.')
    parser.add_argument('--input', type=str, required=True, help='Path to input JSONL file containing reviews')
    parser.add_argument('--inappropriate_words', type=str, required=True, help='Path to text file with inappropriate words')
    parser.add_argument('--output', type=str, required=True, help='Path to output JSONL file for filtered reviews')
    # parser.add_argument('--aggregations', type=str, required=True, help='Path to output JSONL file for aggregations')
    args = parser.parse_args()

    # Read reviews from input JSONL file
    file_path = "data\\"

    #load reviews and schema file
    reviews = read_jsonl_file(file_path+args.input)

    with open('schemas/review.json', 'r') as file:
        schema = json.load(file)

    # Load the jsonl file into a pandas DataFrame
    # df_review = pd.read_json(file_path+args.input, lines=True)
    np_review = validate_schema(reviews, schema)
    # df_review['publishedAt'] = pd.to_datetime(df_review['publishedAt'],format='mixed', utc=True)

    # load inappropriate words
    # df_inapt_word = pd.read_csv(file_path+args.inappropriate_words
    #                             , encoding='utf-8', 
    #                             delimiter='\t',
    #                             header=None,
    #                             names =["inapt_words"])  # Adjust the delimiter if necessary
    np_inapt_word = np.genfromtxt(file_path+args.inappropriate_words, 
                                  dtype='str', encoding='utf-8')

    #replace inappropriate words
    filtered_review = replace_foul_words(np_review, np_inapt_word)

    df_fil_review = pd.DataFrame(filtered_review)
    df_fil_review['publishedAt'] = pd.to_datetime(df_fil_review['publishedAt'],format='mixed', utc=True)
    # print(df_fil_review.info())
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
    today = datetime.now(timezone.utc)

    # Calculate the date three years ago from today using relativedelta
    three_years_ago = today - relativedelta(years=3)
    # print(three_years_ago.date())
    df_fil_review = df_fil_review[(df_fil_review['publishedAt'].dt.date>=three_years_ago.date())&
                                  (df_fil_review['percentage']<0.2)]
    
    df_fil_review = df_fil_review.drop(columns=['percentage'])


    df_fil_review.to_json(f"data/{args.output}", orient='records', lines=True, date_format='iso')
    # write_jsonl_file(df_fil_review, f"data\{args.output}")

    #aggregate the values
    # aggregate_values(df_fil_review)
    


    # Display the DataFrame
    print(df_fil_review.info())
    # print("*"+5)

    #process the reviews   


    # filtered_reviews = reviews
    # print(filtered_reviews)

    # Write filtered reviews to output JSONL file
    # write_jsonl_file(filtered_reviews, f"data\{args.output}")

    # print(f"Filtered reviews written to {args.output}")

if __name__ == '__main__':
    main()
