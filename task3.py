from pyspark.sql import SparkSession

from pyspark.sql.functions import from_json, col, to_timestamp, window, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingWindowedAnalytics").getOrCreate()

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse JSON data into columns using the defined schema
parsed_data = lines.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Convert timestamp column to TimestampType and add a watermark
# Watermark allows handling late-arriving data up to 1 minute late
timestamped_data = parsed_data \
    .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .withWatermark("event_time", "1 minute")

# Perform windowed aggregation: sum of fare_amount over a 5-minute window sliding by 1 minute
windowed_aggregation = timestamped_data \
    .groupBy(window(col("event_time"), "5 minutes", "1 minute")) \
    .agg(spark_sum("fare_amount").alias("total_fare"))

# Extract window start and end times as separate columns
result = windowed_aggregation.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("total_fare")
)

# Define a function to write each batch to a CSV file with column names
def write_batch_to_csv(batch_df, batch_id):
    # Save the batch DataFrame as a CSV file with headers included
    if batch_df.count() > 0:
        batch_df.coalesce(1) \
            .write \
            .mode("append") \
            .option("header", "true") \
            .csv("outputs/task_3")

# Use foreachBatch to apply the function to each micro-batch
query = result.writeStream \
    .foreachBatch(write_batch_to_csv) \
    .outputMode("update") \
    .option("checkpointLocation", "checkpoints/task_3") \
    .trigger(processingTime="10 seconds") \
    .start()

# Also print to console for verification
console_query = result.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
