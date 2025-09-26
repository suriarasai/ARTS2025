from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import from_json, col, to_timestamp, window, expr, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
spark = SparkSession \
        .builder \
        .appName("Tumbling Window Demo") \
        .master("local[*]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 2) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()
# Define the schema for the stock JSON data
stockSchema = StructType([
    StructField("CreatedTime", StringType(), True),
    StructField("Type", StringType()),
    StructField("Amount", IntegerType()),
    StructField("BrokerCode", StringType())])
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "trades") \
    .option("startingOffsets", "earliest") \
    .load()

value_df = kafka_df.select(from_json(col("value").cast("string"), stockSchema).alias("value"))

trade_df = value_df.select("value.*") \
    .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("Buy", expr("case when Type == 'BUY' then Amount else 0 end")) \
    .withColumn("Sell", expr("case when Type == 'SELL' then Amount else 0 end"))

window_agg_df = trade_df \
    .groupBy(  # col("BrokerCode"),
    window(col("CreatedTime"), "15 minute")) \
    .agg(sum("Buy").alias("TotalBuy"),
         sum("Sell").alias("TotalSell"))

output_df = window_agg_df.select("window.start", "window.end", "TotalBuy", "TotalSell")

'''
running_total_window = Window.orderBy("end") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

final_output_df = output_df \
    .withColumn("RTotalBuy", sum("TotalBuy").over(running_total_window)) \
    .withColumn("RTotalSell", sum("TotalSell").over(running_total_window)) \
    .withColumn("NetValue", expr("RTotalBuy - RTotalSell"))

final_output_df.show(truncate=False)
'''
window_query = output_df.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("checkpointLocation", "chk-point-dir-trades") \
    .trigger(processingTime="10 seconds") \
    .start()

window_query.awaitTermination()

