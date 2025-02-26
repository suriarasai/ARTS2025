from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, expr, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Configure Kafka packages for Spark.
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

# Create Spark session with necessary configurations.
spark = SparkSession.builder \
    .appName("Watermark Demo") \
    .master("local[3]") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# Define the schema for the incoming JSON data.
stock_schema = StructType([
    StructField("CreatedTime", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("Amount", IntegerType(), True),
    StructField("BrokerCode", StringType(), True)
])

# Read the stream from Kafka.
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "trades") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the JSON data from the Kafka "value" column.
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), stock_schema).alias("data")
).select("data.*")

# Convert the string timestamp to a proper timestamp and create Buy/Sell columns.
trade_df = parsed_df \
    .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("Buy", expr("CASE WHEN Type = 'BUY' THEN Amount ELSE 0 END")) \
    .withColumn("Sell", expr("CASE WHEN Type = 'SELL' THEN Amount ELSE 0 END"))

# Apply watermarking to handle late data and perform a windowed aggregation.
# A watermark of 30 minutes allows Spark to drop data that is more than 30 minutes late.
windowed_agg_df = trade_df \
    .withWatermark("CreatedTime", "30 minutes") \
    .groupBy(window(col("CreatedTime"), "15 minutes")) \
    .agg(
        sum("Buy").alias("TotalBuy"),
        sum("Sell").alias("TotalSell")
    )

# Select and rename window fields for clarity.
result_df = windowed_agg_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "TotalBuy",
    "TotalSell"
)

# Write the aggregated results to the console in update mode.
query = result_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("checkpointLocation", "chk-point-dir") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()
