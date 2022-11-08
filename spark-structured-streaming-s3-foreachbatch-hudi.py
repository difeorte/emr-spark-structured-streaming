import sys
from pyspark.sql import SparkSession

HUDI_FORMAT = "org.apache.hudi"
hudi_dataset = "s3://<your-bucket-with-hudi-dataset>/hudi_trips_table/*"
streaming_dataset = "s3://<your-bucket-fed-in-real-time>/streaming_data/"

def foreach_batch_function(df, epoch_id):
    hudi_df=spark.read.format(HUDI_FORMAT).load(hudi_dataset)
    hudi_df.show()
    crossed_df = df.join(hudi_df,df.no_alliance == hudi_df.trip_id, how="inner")
    crossed_df.show()

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("StructuredStreamingS3")\
        .getOrCreate()

    schema = "`no_alliance` INT, `description_alliance` STRING, `latitude` STRING, `longitude` STRING, `expiry_date` STRING"

    s3DF = spark \
        .readStream \
        .format("csv") \
        .schema(schema) \
        .option("header", "true") \
        .option("maxFilesPerTrigger", "10") \
        .load(streaming_dataset)

    query = s3DF \
        .writeStream\
        .outputMode('append')\
        .foreachBatch(foreach_batch_function)\
        .trigger(processingTime='10 seconds')\
        .start()
        
    query.awaitTermination()
