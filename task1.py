from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics").getOrCreate()

# Define the schema for incoming JSON data
# Note: driver_id comes as integer from data generator
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

# Write parsed data to CSV files in outputs/task_1
query = parsed_data.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "outputs/task_1") \
    .option("checkpointLocation", "checkpoints/task_1") \
    .option("header", "true") \
    .trigger(processingTime="5 seconds") \
    .start()

# Also print to console for verification (optional)
console_query = parsed_data.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()