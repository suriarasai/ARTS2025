from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSlidingWindowExample") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Kafka connection parameters
kafka_bootstrap_servers = "localhost:9092"  # Adjust with your Kafka server(s)
kafka_topic = "my-topic"             # Replace with your Kafka topic

# Read streaming data from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Assume the Kafka message value is a JSON string. Define its schema.
json_schema = StructType([
    StructField("event_time", TimestampType(), True),
    StructField("value", StringType(), True)
])

# Convert the binary 'value' column to string and then parse JSON
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), json_schema).alias("data")) \
    .select("data.*")

# Use a sliding window to aggregate events.
# Here, we define a window of 1 minutes that slides every 5 seconds.
windowed_counts = json_df \
    .withWatermark("event_time", "60 seconds") \
    .groupBy(window(col("event_time"), "60 seconds", "30 seconds"), col("value")) \
    .count()

# Write the results to the console for debugging purposes.
query = windowed_counts.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
