# import apache_beam as beam

# pipeline = beam.Pipeline()

# # Example pipeline code
# lines = pipeline | beam.Create(['Hello', 'World'])
# lines | beam.io.WriteToText('output.txt')

# pipeline.run()

# import asyncio

# async def count_to(number, delay,task_name):
#     for i in range(1, number + 1):
#         print(i,task_name)
#         await asyncio.sleep(delay)

# async def main():
#     task1 = asyncio.create_task(count_to(10, 1,"T1"))
#     task2 = asyncio.create_task(count_to(5, 2,"T2"))

#     await asyncio.gather(task1, task2)

# asyncio.run(main())