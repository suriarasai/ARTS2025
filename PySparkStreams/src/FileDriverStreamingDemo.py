from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession \
        .builder \
        .appName("File Streaming Demo") \
        .master("local[*]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

raw_df = spark.readStream \
        .format("json") \
        .option("path", "rebu\\driver") \
        .option("maxFilesPerTrigger", 1) \
        .option("inferSchema", "True") \
        .option("multiLine", "True") \
        .load()
# raw_df.printSchema()
driverWriterQuery = raw_df.writeStream \
        .format("json") \
        .queryName("Flattened Driver Writer") \
        .outputMode("append") \
        .option("path", "driver-output") \
        .option("checkpointLocation", "chk-point-dir-file-driver") \
        .trigger(processingTime="1 minute") \
        .start()
driverWriterQuery.awaitTermination()


