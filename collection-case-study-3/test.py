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

import numpy as np

# Sample data
np_foul = np.array(['frack' ,'ifracking', 'poop', 'butt' ,'suck','frack'])
clean_word = 'frack'

# Using np.char.find to find the substring
matches = (np.char.find(np_foul, clean_word)>=0)

print(matches)
# Filtering the array based on matches
np_foul[matches] = '****'
filtered_array = np_foul

print("Filtered array:", filtered_array)
