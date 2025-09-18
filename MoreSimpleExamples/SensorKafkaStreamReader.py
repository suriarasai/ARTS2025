from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# Initialize SparkSession
spark = (SparkSession.builder.appName("KafkaConsumer")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
        .getOrCreate())

# Read from the Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert the binary 'value' column to string
df = df.selectExpr("CAST(value AS STRING) as json_str")

# Define the schema for our sensor JSON data
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("creation_time", TimestampType(), True),
    StructField("temperature", IntegerType(), True)
])

# Parse the JSON string into columns
parsed_df = df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Write the output to the console
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
