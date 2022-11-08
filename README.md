# Spark Structured Streaming on Amazon EMR

This is a reference pyspark script to use Spark Structured Streaming on a service such as Amazon EMR to consume from a dataset in Amazon S3 that is being fed in real time. Such dataset is loaded into a streaming dataframe, then it is joined with a slow changing dataset in Hudi format located on Amazon S3. The script uses microbatches of 10 seconds and the foreachbatch method.
