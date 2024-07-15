### Architecture

![ALt text](images/restaurant%20review%20architecture.jpg)


1. Batch processing per day:

    * The system will process incoming reviews in daily batches, ensuring that the data is handled efficiently and within a manageable timeframe. 

2. Receive the JSON reviews files in collection/inputReviews/ymd partitioned buckets:

    * Incoming review files are stored in Google Cloud Storage (GCS) within the collection/inputReviews bucket, organized by year, month, and day (ymd). We need to store the reviews in accordance to date.

3. Inappropriate words go to collection/inappropriateWords bucket:

    * The list of inappropriate words is stored in the collection/inappropriateWords bucket in GCS. The inappropriate words can be independently updated by anyone.

4. Utilize Airflow to managing our pipeline:

    * Apache Airflow can be used to orchestrate the daily batch processing pipeline, handling task scheduling, execution, and monitoring.

5. We write our pipeline code in Github:
    * Git is employed for source code management, allowing for efficient version control, collaboration, and deployment. 

6. Github main branch deployment to SRC in GCS:

    * All pipeline related code is maintained in the SRC bucket in GCS. This is where Airflow DAG discovery runs and we see our pipelines in Airflow UI.

7. We get the reviews and inappropriate words from the buckets:

    * During processing, the Airflow retrieves review files from the collection/inputReviews bucket and the inappropriate words list from the collection/inappropriateWords bucket. This setup ensures that the necessary data is readily available for the filtering and processing tasks.

8. Then we apply the required filtering and aggregation tasks:

    * We can write a python operator as done in 2nd task applies all filtering and aggregation tasks to the reviews, such as removing outdated reviews, detecting inappropriate content, and performing schema validation. 

9. Write the result to BigQuery for all 3 tables:

    * Processed data is written to BigQuery, facilitating efficient storage and querying. This will make it easier to aggregate the data everyday (less reading from external storage bucket). We will have 3 tables processed_reviews(partition by ymd), discarded_reviews(partition by ymd) and restaurant_reviews_aggregates.

10. Export the processed review and discarded reviews to GCS:

    * After processing, the next task would be to export valid processed reviews and discarded reviews to designated buckets in GCS. That are collection/processedReviews/ymd and collection/discardedReviews/ymd.