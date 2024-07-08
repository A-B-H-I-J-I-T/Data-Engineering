import json
import numpy as np

# Example JSONL data
jsonl_data = '''
{"name": "Alice", "age": 25, "height": 5.5}
{"name": "Bob", "age": 30, "height": 6.0}
{"name": "Charlie", "age": 35, "height": 5.8}
'''

# Read and parse the JSONL data
data_list = [json.loads(line) for line in jsonl_data.strip().split('\n')]

print(data_list)
# Define the structure of the array based on the JSON keys and their types
dtype = [('name', 'U10'), ('age', 'i4'), ('height', 'f4')]

# Create an empty structured array
data = np.zeros(len(data_list), dtype=dtype)

# Populate the array with data
for i, item in enumerate(data_list):
    data[i] = (item['name'], item['age'], item['height'])

print(data)