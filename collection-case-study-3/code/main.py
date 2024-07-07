import argparse
import json
import os

def read_jsonl_file(file_path):
    """Reads a JSONL file and returns a list of dictionaries."""
    data = []
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            data.append(json.loads(line.strip()))
    return data[0:2]

def write_jsonl_file(data, file_path):
    """Writes a list of dictionaries to a JSONL file."""
    with open(file_path, 'w', encoding='utf-8') as file:
        for item in data:
            file.write(json.dumps(item) + '\n')

# def filter_reviews(reviews, inappropriate_words):
#     """Filters reviews based on inappropriate words."""
#     filtered_reviews = []
#     for review in reviews:
#         if not any(word in review['text'].lower() for word in inappropriate_words):
#             filtered_reviews.append(review)
#     return filtered_reviews

# def compute_aggregations(reviews):
#     """Compute aggregations from reviews (example: count of reviews)."""
#     aggregation_result = {
#         'total_reviews': len(reviews)
#         # You can add more aggregations here based on your requirements
#     }
#     return aggregation_result

def main():
    parser = argparse.ArgumentParser(description='Filter and aggregate reviews from JSONL file.')
    parser.add_argument('--input', type=str, required=True, help='Path to input JSONL file containing reviews')
    # parser.add_argument('--inappropriate_words', type=str, required=True, help='Path to text file with inappropriate words')
    parser.add_argument('--output', type=str, required=True, help='Path to output JSONL file for filtered reviews')
    # parser.add_argument('--aggregations', type=str, required=True, help='Path to output JSONL file for aggregations')
    args = parser.parse_args()

    # Read reviews from input JSONL file
    reviews = read_jsonl_file(f"data\input_reviews\{args.input}")

    # Read inappropriate words from text file
    # with open(args.inappropriate_words, 'r', encoding='utf-8') as words_file:
    #     inappropriate_words = set(word.strip().lower() for word in words_file.readlines() if word.strip())

    # Filter reviews based on inappropriate words
    # filtered_reviews = filter_reviews(reviews, inappropriate_words)
    filtered_reviews = reviews
    # Write filtered reviews to output JSONL file
    write_jsonl_file(filtered_reviews, f"data\processed_reviews\{args.output}")

    # Compute aggregations
    # aggregations = compute_aggregations(filtered_reviews)

    # Write aggregations to aggregations JSONL file
    # write_jsonl_file([aggregations], args.aggregations)

    print(f"Filtered reviews written to {args.output}")
    # print(f"Aggregations written to {args.aggregations}")

if __name__ == '__main__':
    main()
