### Daigram 1


1. Batch processing per day.
2. Receive the json reviews file in Bucket.
3. Inappropriate words in separate bucket.
4. Utilize Airflow to manage our pipeline.
5. We write our processing code in the src bucket.
6. We use git for source code management and deployment
7. We get the reviews and Inappropriate words from the buckets.
8. The we apply the required filtering and aggregation.
9. Write the result to BigQuery for all 3 tables.
10. export the processed review and discarded reviews to GCS.


### Scaling
 
 1. Data Volume is large
 2. Logic not optimized
 3. Executer is failing
 4. partition by restaurant id?