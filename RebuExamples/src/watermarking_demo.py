# src/watermarking_demo.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os


def create_spark_session():
    """Create Spark session with Kafka support"""
    return SparkSession.builder \
        .appName("TaxiWatermarkingDemo") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.extraJavaOptions",
                "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED") \
        .config("spark.executor.extraJavaOptions",
                "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED") \
        .getOrCreate()


def define_taxi_schema():
    """Define schema for taxi trip data"""
    return StructType([
        StructField("trip_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("processing_time", StringType(), True),
        StructField("pickup_district", StringType(), True),
        StructField("dropoff_district", StringType(), True),
        StructField("distance_km", DoubleType(), True),
        StructField("duration_seconds", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("taxi_type", StringType(), True),
        StructField("taxi_capacity", IntegerType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("hour_of_day", IntegerType(), True),
        StructField("day_of_week", StringType(), True),
        StructField("is_late_data", BooleanType(), True),
        StructField("late_by_minutes", IntegerType(), True)
    ])


def main():
    print("🚕 Starting Watermarking Demonstration...")
    print("=" * 60)
    print("This demo shows how watermarking handles late-arriving data")
    print("=" * 60)

    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", "taxi-trips") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON data
    schema = define_taxi_schema()

    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Convert timestamps
    parsed_df = parsed_df.withColumn(
        "event_timestamp",
        to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    ).withColumn(
        "processing_timestamp",
        to_timestamp(col("processing_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    )

    print("📊 WATERMARKING CONFIGURATIONS")
    print("=" * 50)

    # 1. STRICT WATERMARK (30 seconds) - Drops late data quickly
    strict_watermark = parsed_df \
        .withWatermark("event_timestamp", "30 seconds") \
        .groupBy(
        window(col("event_timestamp"), "2 minutes"),
        col("pickup_district")
    ) \
        .agg(
        count("trip_id").alias("trip_count"),
        sum("fare_amount").alias("total_revenue"),
        approx_count_distinct(col("trip_id")).alias("unique_trips"),
        collect_list(
            when(col("is_late_data") == True, col("late_by_minutes")).otherwise(None)
        ).alias("late_data_minutes")
    ) \
        .withColumn("watermark_type", lit("STRICT_30s")) \
        .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("watermark_type"),
        col("pickup_district"),
        col("trip_count"),
        round(col("total_revenue"), 2).alias("total_revenue"),
        col("unique_trips"),
        col("late_data_minutes")
    )

    # 2. LENIENT WATERMARK (5 minutes) - Accepts more late data
    lenient_watermark = parsed_df \
        .withWatermark("event_timestamp", "5 minutes") \
        .groupBy(
        window(col("event_timestamp"), "2 minutes"),
        col("pickup_district")
    ) \
        .agg(
        count("trip_id").alias("trip_count"),
        sum("fare_amount").alias("total_revenue"),
        approx_count_distinct(col("trip_id")).alias("unique_trips"),
        sum(when(col("is_late_data") == True, 1).otherwise(0)).alias("late_events_count"),
        collect_list(
            when(col("is_late_data") == True, col("late_by_minutes")).otherwise(None)
        ).alias("late_data_minutes")
    ) \
        .withColumn("watermark_type", lit("LENIENT_5m")) \
        .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("watermark_type"),
        col("pickup_district"),
        col("trip_count"),
        round(col("total_revenue"), 2).alias("total_revenue"),
        col("unique_trips"),
        col("late_events_count"),
        col("late_data_minutes")
    )

    # 3. NO WATERMARK - Keeps all data indefinitely (memory issues in production)
    no_watermark = parsed_df \
        .groupBy(
        window(col("event_timestamp"), "2 minutes"),
        col("pickup_district")
    ) \
        .agg(
        count("trip_id").alias("trip_count"),
        sum("fare_amount").alias("total_revenue"),
        sum(when(col("is_late_data") == True, 1).otherwise(0)).alias("late_events_count"),
        max(col("late_by_minutes")).alias("max_lateness_minutes")
    ) \
        .withColumn("watermark_type", lit("NO_WATERMARK")) \
        .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("watermark_type"),
        col("pickup_district"),
        col("trip_count"),
        round(col("total_revenue"), 2).alias("total_revenue"),
        col("late_events_count"),
        col("max_lateness_minutes")
    )

    # 4. LATE DATA DETECTION QUERY
    late_data_analysis = parsed_df \
        .withWatermark("event_timestamp", "2 minutes") \
        .withColumn("lateness_seconds",
                    unix_timestamp(col("processing_timestamp")) -
                    unix_timestamp(col("event_timestamp"))) \
        .withColumn("lateness_category",
                    when(col("lateness_seconds") <= 30, "ON_TIME")
                    .when(col("lateness_seconds") <= 120, "SLIGHTLY_LATE")
                    .when(col("lateness_seconds") <= 300, "MODERATELY_LATE")
                    .otherwise("VERY_LATE")) \
        .groupBy(
        window(col("event_timestamp"), "1 minute"),
        col("lateness_category")
    ) \
        .agg(
        count("trip_id").alias("event_count"),
        avg("lateness_seconds").alias("avg_lateness_sec"),
        max("lateness_seconds").alias("max_lateness_sec")
    ) \
        .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("lateness_category"),
        col("event_count"),
        round(col("avg_lateness_sec"), 1).alias("avg_lateness_sec"),
        col("max_lateness_sec")
    )

    # Start output queries
    print("🔄 Starting watermarking analysis queries...")

    query1 = strict_watermark.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='10 seconds') \
        .queryName("StrictWatermark") \
        .start()

    query2 = lenient_watermark.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='12 seconds') \
        .queryName("LenientWatermark") \
        .start()

    query3 = no_watermark.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 5) \
        .trigger(processingTime='15 seconds') \
        .queryName("NoWatermark") \
        .start()

    query4 = late_data_analysis.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='8 seconds') \
        .queryName("LateDataAnalysis") \
        .start()

    print("✅ All watermarking queries started!")
    print("\n📋 Running watermarking comparisons:")
    print("   • STRICT (30s): Drops late data quickly, low memory usage")
    print("   • LENIENT (5m): Accepts more late data, higher memory usage")
    print("   • NO WATERMARK: Keeps everything, potential memory issues")
    print("   • LATE DATA ANALYSIS: Categorizes data by arrival lateness")

    print("\n🎯 Key Watermarking Concepts:")
    print("   • Watermark = Max event time seen - watermark delay")
    print("   • Data older than watermark gets dropped")
    print("   • Trade-off between completeness and resource usage")
    print("   • Choose delay based on your late data tolerance")

    print("\n🔄 Processing streaming data... Press Ctrl+C to stop")

    try:
        # Wait for termination
        query1.awaitTermination()
    except KeyboardInterrupt:
        print("\n⏹️  Stopping watermarking demonstration...")
        query1.stop()
        query2.stop()
        query3.stop()
        query4.stop()
        spark.stop()


if __name__ == "__main__":
    main()