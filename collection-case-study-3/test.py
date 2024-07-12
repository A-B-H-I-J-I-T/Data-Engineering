# import json
# import numpy as np

# # Example JSONL data
# jsonl_data = '''
# {"name": "Alice", "age": 25, "height": 5.5}
# {"name": "Bob", "age": 30, "height": 6.0}
# {"name": "Charlie", "age": 35, "height": 5.8}
# '''

# # Read and parse the JSONL data
# data_list = [json.loads(line) for line in jsonl_data.strip().split('\n')]

# print(data_list)
# # Define the structure of the array based on the JSON keys and their types
# dtype = [('name', 'U10'), ('age', 'i4'), ('height', 'f4')]

# # Create an empty structured array
# data = np.zeros(len(data_list), dtype=dtype)

# # Populate the array with data
# for i, item in enumerate(data_list):
#     data[i] = (item['name'], item['age'], item['height'])

# print(data)

# import numpy as np

# # Sample data
# np_foul = np.array(['frack' ,'ifracking', 'poop', 'butt' ,'suck','frack'])
# clean_word = 'frack'

# # Using np.char.find to find the substring
# matches = (np.char.find(np_foul, clean_word)>=0)

# print(matches)
# # Filtering the array based on matches
# np_foul[matches] = '****'
# filtered_array = np_foul

# print("Filtered array:", filtered_array)

# import pandas as pd

# # Sample DataFrame
# data = {
#     'restaurant_id': [1, 1, 1, 2, 2, 3],
#     'review_score': [5, 4, 3, 5, 2, 4],
#     'review_text': ['Great food!', 'Nice place.', 'Okay experience.', 'Loved it!', 'Not good.', 'Pretty decent.'],
#     'review_date': pd.to_datetime(['2023-01-01', '2023-01-05', '2023-01-10', '2023-01-15', '2023-01-20', '2023-01-25'])
# }

# df = pd.DataFrame(data)

# # Calculate review length
# df['review_length'] = df['review_text'].apply(len)

# # Calculate review age in days
# today = pd.to_datetime('today')
# print(today)
# df['review_age'] = (today - df['review_date']).dt.days

# # Group by restaurant_id and aggregate
# agg_funcs = {
#     'review_score': ['count', 'mean'],
#     'review_length': 'mean',
#     'review_age': ['min', 'max', 'mean']
# }

# grouped = df.groupby('restaurant_id').agg(agg_funcs)

# # Flatten the MultiIndex columns
# # grouped.columns = ['_'.join(col).strip() for col in grouped.columns.values]
# # grouped = grouped.reset_index()

# print(pd.Timestamp.now(tz='UTC').normalize())


# import pandas as pd

# # Create a DataFrame with two dates
# df = pd.DataFrame({
#     'date1': pd.to_datetime(['2022-01-01', '2022-01-15']),
#     'date2': pd.to_datetime(['2022-01-15', '2022-01-30'])
# })

# # Calculate the number of days between the two dates
# df['num_days'] = (df['date2'] - df['date1']).dt.days

# print(pd.Timestamp.now(tz='UTC').normalize())


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
    with open(file_path, 'r') as file:
        for line in file:
            data.append(json.loads(line.strip()))
    return data


def main():
    df_review = pd.read_json('data/reviews.jsonl',lines=True)
    repeat_times = (100 // len(df_review)) + 1

    df_large = pd.concat([df_review] * repeat_times, ignore_index=True)

    df_large.to_json('test_revews.jsonl', orient='records', lines=True, date_format='iso')
    # print(df_large)

if __name__ == '__main__':
    main()
