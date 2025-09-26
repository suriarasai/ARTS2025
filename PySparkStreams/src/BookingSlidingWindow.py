from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
# Initialize a Spark session
spark = (SparkSession.builder.appName("TaxiBookingStream") \
         .config("spark.streaming.stopGracefullyOnShutdown", "true") \
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
         .getOrCreate())

# Define schema for the booking records
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType

schema = StructType([
    StructField("BookingID", IntegerType(), True),
    StructField("MessageSubmittedTime", LongType(), True),
    StructField("MessageReceivedTime", LongType(), True),
    StructField("CustomerID", IntegerType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("PhoneNumber", LongType(), True),
    StructField("PickUpLocation", StringType(), True),
    StructField("PickupPostcode", IntegerType(), True),
    StructField("PickUpTime", StringType(), True),
    StructField("DropLocation", StringType(), True),
    StructField("DropPostcode", IntegerType(), True),
    StructField("TaxiType", StringType(), True),
    StructField("FareType", StringType(), True),
    StructField("Fare", StringType(), True)
])

# Read the stream from Kafka
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "booking") \
    .load()

# Parse the JSON data from the Kafka stream
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Convert MessageSubmittedTime to timestamp (assuming it's in milliseconds)
parsed_df = parsed_df.withColumn("MessageSubmittedTime", (col("MessageSubmittedTime") / 1000).cast("timestamp"))

# Apply a tumbling window of 10 minutes on the MessageSubmittedTime
windowed_df = parsed_df.groupBy(window(col("MessageSubmittedTime"), "100 minutes", slideDuration="10 minutes")).count()

# Output the result to the console
query = windowed_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Await the termination of the stream
query.awaitTermination()