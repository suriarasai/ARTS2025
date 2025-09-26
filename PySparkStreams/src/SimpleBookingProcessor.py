from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql.types import StructField

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TaxiBookingProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "taxi_bookings"

# Read streaming data from Kafka
taxi_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("startingOffsets", "earliest") \
    .option("subscribe", kafka_topic) \
     .load()

# Define the schema for the booking data
booking_schema = StructType([
    StructField("booking_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("pickup_location", StringType(), True),
    StructField("dropoff_location", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# Parse the JSON data and extract values
parsed_df = taxi_stream_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), booking_schema).alias("data")) \
    .select("data.*")

# Convert the timestamp to a TimestampType
parsed_df = parsed_df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

# Example processing 1: Count number of bookings by pickup location in real time
bookings_by_location = parsed_df.groupBy("pickup_location").count()

# Example processing 2: Calculate bookings per minute window
bookings_per_minute = parsed_df \
    .groupBy(window(col("timestamp"), "1 minute"), col("pickup_location")) \
    .count() \
    .select("window", "pickup_location", "count")

# Define the output sink (console output in this case)
query = bookings_per_minute.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
