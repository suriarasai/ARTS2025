# src/sliding_window_consumer.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os


def create_spark_session():
    """Create Spark session with Kafka support"""
    return SparkSession.builder \
        .appName("TaxiSlidingWindowAnalysis") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
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
    print("🚕 Starting Sliding Window Analysis...")

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

    # Convert event_time to timestamp
    parsed_df = parsed_df.withColumn(
        "event_timestamp",
        to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    )

    print("📊 SLIDING WINDOW AGGREGATIONS")
    print("=" * 50)

    # 1. Sliding Window: Trip trends with 5-minute window, 1-minute slide
    sliding_trends = parsed_df \
        .withWatermark("event_timestamp", "2 minutes") \
        .groupBy(
        window(col("event_timestamp"), "5 minutes", "1 minute")
    ) \
        .agg(
        count("trip_id").alias("trip_count"),
        sum("fare_amount").alias("total_revenue"),
        avg("fare_amount").alias("avg_fare"),
        avg("distance_km").alias("avg_distance"),
        approx_count_distinct("pickup_district").alias("unique_pickup_districts")
    ) \
        .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("trip_count"),
        round(col("total_revenue"), 2).alias("total_revenue"),
        round(col("avg_fare"), 2).alias("avg_fare"),
        round(col("avg_distance"), 2).alias("avg_distance"),
        col("unique_pickup_districts")
    )

    # 2. Sliding Window: District popularity with 4-minute window, 30-second slide
    sliding_districts = parsed_df \
        .withWatermark("event_timestamp", "2 minutes") \
        .groupBy(
        window(col("event_timestamp"), "4 minutes", "30 seconds"),
        col("pickup_district")
    ) \
        .agg(
        count("trip_id").alias("pickup_count"),
        sum("fare_amount").alias("district_revenue"),
        avg("passenger_count").alias("avg_passengers")
    ) \
        .where(col("pickup_count") >= 2) \
        .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("pickup_district"),
        col("pickup_count"),
        round(col("district_revenue"), 2).alias("revenue"),
        round(col("avg_passengers"), 1).alias("avg_passengers")
    )

    # 3. Sliding Window: Real-time taxi performance with 3-minute window, 30-second slide
    sliding_performance = parsed_df \
        .withWatermark("event_timestamp", "2 minutes") \
        .groupBy(
        window(col("event_timestamp"), "3 minutes", "30 seconds"),
        col("taxi_type")
    ) \
        .agg(
        count("trip_id").alias("trip_count"),
        avg("fare_amount").alias("avg_fare"),
        sum("distance_km").alias("total_distance"),
        avg(col("duration_seconds") / 60).alias("avg_duration_minutes"),
        sum("passenger_count").alias("total_passengers")
    ) \
        .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("taxi_type"),
        col("trip_count"),
        round(col("avg_fare"), 2).alias("avg_fare"),
        round(col("total_distance"), 2).alias("total_distance"),
        round(col("avg_duration_minutes"), 1).alias("avg_duration_min"),
        col("total_passengers")
    )

    # 4. Sliding Window: Rush hour detection with 6-minute window, 2-minute slide
    rush_hour_detection = parsed_df \
        .withWatermark("event_timestamp", "2 minutes") \
        .groupBy(
        window(col("event_timestamp"), "6 minutes", "2 minutes")
    ) \
        .agg(
        count("trip_id").alias("trip_count"),
        avg("fare_amount").alias("avg_fare"),
        approx_count_distinct("pickup_district").alias("active_districts"),
        sum(when(col("passenger_count") > 2, 1).otherwise(0)).alias("group_trips"),
        avg("distance_km").alias("avg_trip_distance")
    ) \
        .withColumn(
        "is_rush_hour",
        when(col("trip_count") > 15, "HIGH_DEMAND").otherwise("NORMAL")
    ) \
        .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("trip_count"),
        col("is_rush_hour"),
        round(col("avg_fare"), 2).alias("avg_fare"),
        col("active_districts"),
        col("group_trips"),
        round(col("avg_trip_distance"), 2).alias("avg_distance")
    )

    # Output queries
    query1 = sliding_trends.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 10) \
        .trigger(processingTime='8 seconds') \
        .queryName("SlidingTrendsWindow") \
        .start()

    query2 = sliding_districts.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 15) \
        .trigger(processingTime='12 seconds') \
        .queryName("SlidingDistrictsWindow") \
        .start()

    query3 = sliding_performance.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='10 seconds') \
        .queryName("SlidingPerformanceWindow") \
        .start()

    query4 = rush_hour_detection.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='15 seconds') \
        .queryName("RushHourDetection") \
        .start()

    print("✅ Sliding window queries started!")
    print("📋 Running analyses:")
    print("   • 5-min window, 1-min slide: Trip trends")
    print("   • 4-min window, 30-sec slide: District popularity")
    print("   • 3-min window, 30-sec slide: Taxi performance")
    print("   • 6-min window, 2-min slide: Rush hour detection")
    print("\n🔄 Processing streaming data... Press Ctrl+C to stop")

    try:
        # Wait for all queries to terminate
        query1.awaitTermination()
    except KeyboardInterrupt:
        print("\n⏹️  Stopping sliding window analysis...")
        query1.stop()
        query2.stop()
        query3.stop()
        query4.stop()
        spark.stop()


if __name__ == "__main__":
    main()