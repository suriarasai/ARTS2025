from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("Sliding Window Demo") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 1) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()

schema = StructType([
    StructField("CreatedTime", StringType()),
    StructField("Reading", DoubleType())
])

kafka_source_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor") \
    .load()

value_df = kafka_source_df.select(col("key").cast("string").alias("SensorID"),
                                  from_json(col("value").cast("string"), schema).alias("value"))

sensor_df = value_df.select("SensorID", "value.*") \
    .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))
sensor_df.printSchema()
agg_df = sensor_df \
    .withWatermark("CreatedTime", "30 minute") \
    .groupBy(col("SensorID"),
             window(col("CreatedTime"), "15 minute", "5 minute")) \
    .agg(max("Reading").alias("MaxReading"))

output_df = agg_df.select("SensorID", "window.start", "window.end", "MaxReading")

window_query = output_df.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("checkpointLocation", "chk-point-dir") \
    .trigger(processingTime="1 minute") \
    .start()

window_query.awaitTermination()
