# Spark Structured Streaming on Amazon EMR

Reference pyspark script to use Spark Structured Streaming in a service such as EMR to consume from a dataset in Amazon S3 that is being fed in real time. Such dataset is loaded into as a streaming dataframe to be joined with a slow changing dataset in Hudi format. The script uses microbatches of 10 seconds and the foreachbatch method.
