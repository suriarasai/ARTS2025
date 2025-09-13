from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession \
        .builder \
        .appName("File Streaming Demo") \
        .master("local[*]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
        .getOrCreate()

raw_df = spark.readStream \
        .format("csv") \
        .option("path", "data\\booking") \
        .option("maxFilesPerTrigger", 1) \
        .option("inferSchema", "True") \
        .load()

# Write the streaming data to Kafka
query = raw_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "booking-demo") \
    .option("checkpointLocation", "/tmp/booking-kafka-producer-checkpoint") \
    .start()

query.awaitTermination()


