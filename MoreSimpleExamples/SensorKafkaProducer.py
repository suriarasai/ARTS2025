from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit, rand, to_json, struct

# Initialize SparkSession
spark = (SparkSession.builder.appName("KafkaProducer") \
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
         .getOrCreate())

# Create a streaming DataFrame using the 'rate' source (one row per second)
df = spark.readStream.format("rate").option("rowsPerSecond", 1).load()

# Simulate sensor IDs based on the 'value' column (cycling through three sensors)
df_with_sensors = df.withColumn("sensor_id",
    when((col("value") % 3) == 0, lit("sensor_1"))
    .when((col("value") % 3) == 1, lit("sensor_2"))
    .otherwise(lit("sensor_3"))
)

# Use the generated timestamp as creation_time
df_with_sensors = df_with_sensors.withColumn("creation_time", col("timestamp"))

# Generate a temperature between 25 and 34 degrees Celsius
df_with_sensors = df_with_sensors.withColumn("temperature", (rand() * 9 + 25).cast("integer"))

# Convert the sensor data to JSON format for Kafka
kafka_df = df_with_sensors.selectExpr(
    "CAST(sensor_id AS STRING) as key",
    "to_json(struct(sensor_id, creation_time, temperature)) as value"
)

# Write the streaming data to Kafka
query = kafka_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "sensor") \
    .option("checkpointLocation", "/tmp/kafka-producer-checkpoint") \
    .option("kafka.acks", "all") \
    .option("kafka.retries", 3) \
    .option("kafka.batch.size", 16384) \
    .option("kafka.linger.ms", 1) \
    .start()

query.awaitTermination()
