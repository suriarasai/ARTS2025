# src/tumbling_window_consumer.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os


def create_spark_session():
    """Create Spark session with Kafka support"""
    return SparkSession.builder \
        .appName("TaxiTumblingWindowAnalysis") \
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
    print("🚕 Starting Tumbling Window Analysis...")

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

    print("📊 TUMBLING WINDOW AGGREGATIONS")
    print("=" * 50)

    # 1. Tumbling Window: Trip count and revenue by 2-minute windows
    tumbling_trips = parsed_df \
        .withWatermark("event_timestamp", "2 minutes") \
        .groupBy(
        window(col("event_timestamp"), "2 minutes")
    ) \
        .agg(
        count("trip_id").alias("trip_count"),
        sum("fare_amount").alias("total_revenue"),
        avg("fare_amount").alias("avg_fare"),
        max("distance_km").alias("max_distance")
    ) \
        .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("trip_count"),
        round(col("total_revenue"), 2).alias("total_revenue"),
        round(col("avg_fare"), 2).alias("avg_fare"),
        col("max_distance")
    )

    # 2. Tumbling Window: Popular districts by 3-minute windows
    tumbling_districts = parsed_df \
        .withWatermark("event_timestamp", "2 minutes") \
        .groupBy(
        window(col("event_timestamp"), "3 minutes"),
        col("pickup_district")
    ) \
        .agg(
        count("trip_id").alias("pickup_count"),
        avg("fare_amount").alias("avg_fare_from_district")
    ) \
        .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("pickup_district"),
        col("pickup_count"),
        round(col("avg_fare_from_district"), 2).alias("avg_fare")
    )

    # 3. Tumbling Window: Taxi type performance by 5-minute windows
    tumbling_taxi_types = parsed_df \
        .withWatermark("event_timestamp", "2 minutes") \
        .groupBy(
        window(col("event_timestamp"), "5 minutes"),
        col("taxi_type")
    ) \
        .agg(
        count("trip_id").alias("trip_count"),
        sum("fare_amount").alias("revenue"),
        avg("distance_km").alias("avg_distance"),
        avg("duration_seconds").alias("avg_duration")
    ) \
        .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("taxi_type"),
        col("trip_count"),
        round(col("revenue"), 2).alias("revenue"),
        round(col("avg_distance"), 2).alias("avg_distance"),
        round(col("avg_duration"), 0).alias("avg_duration_sec")
    )

    # Output queries
    query1 = tumbling_trips.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='10 seconds') \
        .queryName("TumblingTripsWindow") \
        .start()

    query2 = tumbling_districts.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='15 seconds') \
        .queryName("TumblingDistrictsWindow") \
        .start()

    query3 = tumbling_taxi_types.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='20 seconds') \
        .queryName("TumblingTaxiTypesWindow") \
        .start()

    print("✅ Tumbling window queries started!")
    print("📋 Running analyses:")
    print("   • 2-minute trip count and revenue windows")
    print("   • 3-minute district popularity windows")
    print("   • 5-minute taxi type performance windows")
    print("\n🔄 Processing streaming data... Press Ctrl+C to stop")

    try:
        # Wait for all queries to terminate
        query1.awaitTermination()
    except KeyboardInterrupt:
        print("\n⏹️  Stopping tumbling window analysis...")
        query1.stop()
        query2.stop()
        query3.stop()
        spark.stop()


if __name__ == "__main__":
    main()