import argparse
import json
import pandas as pd
from jsonschema import validate, ValidationError


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
    for record in data:
        try:
            validate(instance=record, schema=schema)
            valid_data.append(record)  # Add valid record to the list
        except ValidationError as e:
            invalid_data.append(record)  # Ignore invalid records
    print(invalid_data)
    return pd.DataFrame(valid_data)

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
    df_review = validate_schema(reviews, schema)
    df_review['publishedAt'] = pd.to_datetime(df_review['publishedAt'],format='mixed', utc=True)


    # load inappropriate words
    df_inapt_word = pd.read_csv(file_path+args.inappropriate_words
                                , encoding='utf-8', 
                                delimiter='\t',
                                header=None,
                                names =["inapt_words"])  # Adjust the delimiter if necessary


    # Display the DataFrame
    print(df_review)

    #process the reviews   


    # filtered_reviews = reviews
    # print(filtered_reviews)

    # Write filtered reviews to output JSONL file
    # write_jsonl_file(filtered_reviews, f"data\{args.output}")

    # print(f"Filtered reviews written to {args.output}")

if __name__ == '__main__':
    main()
