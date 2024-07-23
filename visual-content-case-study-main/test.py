# # import apache_beam as beam

# # pipeline = beam.Pipeline()

# # # Example pipeline code
# # lines = pipeline | beam.Create(['Hello', 'World'])
# # lines | beam.io.WriteToText('output.txt')

# # pipeline.run()

# # import asyncio

# # async def count_to(number, delay,task_name):
# #     for i in range(1, number + 1):
# #         print(i,task_name)
# #         await asyncio.sleep(delay)

# # async def main():
# #     task1 = asyncio.create_task(count_to(10, 1,"T1"))
# #     task2 = asyncio.create_task(count_to(5, 2,"T2"))

# #     await asyncio.gather(task1, task2)

# # asyncio.run(main())

# import apache_beam as beam

# # Define a function to split sentences into words
# def split_into_words(sentence):
#     return sentence.split()

# # Create a pipeline
# with beam.Pipeline() as pipeline:
#     # Create a PCollection of sentences
#     sentences = pipeline | 'Create Sentences' >> beam.Create([
#         'Apache Beam is powerful',
#         'FlatMap is useful',
#         'Transforms are great'
#     ])

#     # Apply the FlatMap transform to split sentences into words
#     words = sentences | 'Split into Words' >> beam.Map(split_into_words)

#     # Print the output
#     words | beam.Map(print)


# x = (2,{'l':[{'2':'3'}],'r':[{'4':'5'}]})
# z,a = x
# print(z, a)
# # print(x[1]['l'])

from datetime import datetime

def run(created_date):
    today = datetime.today().date()
    created_date = datetime.strptime(created_date, "%Y-%m-%d").date()
    freshness = (today - created_date).days
    score = 1 + (-1 * (freshness / 3650))
    return max(0, min(score, 1))


if __name__ == '__main__':
    print(run("2024-07-22"))