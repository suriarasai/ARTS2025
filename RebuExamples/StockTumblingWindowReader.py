from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json, col, to_timestamp, window

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaStockStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
    .getOrCreate()

# Define the schema for the JSON data
stockSchema = StructType([
    StructField("CreatedTime", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("Amount", IntegerType(), True),
    StructField("BrokerCode", StringType(), True)
])

# Read streaming data from Kafka
rawKafkaDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock") \
    .load()

# Convert the binary 'value' column to string and then parse JSON
jsonDF = rawKafkaDF.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), stockSchema).alias("data")) \
    .select("data.*")

# Convert 'CreatedTime' string to a timestamp column (adjust the format if needed)
streamingDF = jsonDF.withColumn("timestamp", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))

# Apply tumbling window aggregation (e.g., 15-minute windows)
aggregatedDF = streamingDF.groupBy(
    window(col("timestamp"), "15 minutes"),
    col("Type")
).sum("Amount").withColumnRenamed("sum(Amount)", "total_amount")

# Write the aggregated results to the console for debugging/visualization
query = aggregatedDF.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
