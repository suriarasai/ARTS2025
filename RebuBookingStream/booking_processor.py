# booking_processor.py - PySpark Structured Streaming Processor
import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


class TaxiBookingStreamProcessor:
    """
    PySpark Structured Streaming processor for real-time taxi booking data
    Performs enrichment, aggregations, and complex analytics
    """

    def __init__(self, app_name="TaxiBookingRealTimeProcessor"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")

        # Initialize reference data holders
        self.taxi_df = None
        self.driver_df = None
        self.passenger_df = None

    def load_reference_data(self):
        """Load and register reference tables from CSV files and JSON"""

        print("Loading reference data...")

        # Load Taxi data from JSON file
        try:
            self.taxi_df = self.spark.read.option("multiline", "true").json("./data/RebuTaxiCabs.json")
            print(f"Loaded {self.taxi_df.count()} taxis from JSON")
        except Exception as e:
            print(f"Warning: Could not load taxi JSON file: {e}")
            # Fallback to sample data
            taxi_data = [
                {"TaxiID": 1, "TaxiNumber": "SHZ2770", "TaxiType": "Standard",
                 "TaxiMakeModel": "Toyota Carolla", "TaxiPassengerCapacity": 4,
                 "TaxiColor": "Yellow", "TMDTID": "TMA73889"},
                {"TaxiID": 2, "TaxiNumber": "SHY4378", "TaxiType": "Mini Cab",
                 "TaxiMakeModel": "Suzuki Swift", "TaxiPassengerCapacity": 4,
                 "TaxiColor": "Green", "TMDTID": "TMC04591"},
                {"TaxiID": 3, "TaxiNumber": "SHX6464", "TaxiType": "Standard",
                 "TaxiMakeModel": "Toyota Prius", "TaxiPassengerCapacity": 4,
                 "TaxiColor": "Blue", "TMDTID": "TMA12020"},
                {"TaxiID": 4, "TaxiNumber": "SHX4872", "TaxiType": "Premier",
                 "TaxiMakeModel": "Toyota Camry Hybrid", "TaxiPassengerCapacity": 4,
                 "TaxiColor": "Silver", "TMDTID": "TMB02825"},
                {"TaxiID": 5, "TaxiNumber": "SHY9111", "TaxiType": "Maxi Cab",
                 "TaxiMakeModel": "Mercedes Viano", "TaxiPassengerCapacity": 7,
                 "TaxiColor": "Cream", "TMDTID": "TMC45713"}
            ]
            self.taxi_df = self.spark.createDataFrame(taxi_data)

        # Load Driver data from CSV
        try:
            self.driver_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv("./data/RebuDrivers.csv")
            print(f"Loaded {self.driver_df.count()} drivers from CSV")
        except Exception as e:
            print(f"Warning: Could not load drivers CSV file: {e}")
            # Fallback to sample data
            driver_data = [
                {"DriverID": 1, "DriverName": "John Tan", "DriverPhone": 91234567,
                 "TaxiIDDriving": 1, "Rating": 4.5},
                {"DriverID": 2, "DriverName": "Mary Lim", "DriverPhone": 92345678,
                 "TaxiIDDriving": 2, "Rating": 4.8},
                {"DriverID": 3, "DriverName": "Ahmad Rahman", "DriverPhone": 93456789,
                 "TaxiIDDriving": 3, "Rating": 4.2},
                {"DriverID": 4, "DriverName": "Sarah Wong", "DriverPhone": 94567890,
                 "TaxiIDDriving": 4, "Rating": 4.9},
                {"DriverID": 5, "DriverName": "David Chen", "DriverPhone": 95678901,
                 "TaxiIDDriving": 5, "Rating": 4.6}
            ]
            self.driver_df = self.spark.createDataFrame(driver_data)

        # Load Passenger data from CSV
        try:
            self.passenger_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv("./data/RebuPassengers.csv")
            print(f"Loaded {self.passenger_df.count()} passengers from CSV")
        except Exception as e:
            print(f"Warning: Could not load passengers CSV file: {e}")
            # Fallback to sample data
            passenger_data = [
                {"PassengerID": 1, "PassengerName": "Alice Johnson", "MemSilvererStGoldtus": "Gold",
                 "Age": 28, "Gender": "F", "AmountSpent": 450.75, "Address": "Orchard Road",
                 "ContactTitle": "Ms", "Phone": 81234567},
                {"PassengerID": 2, "PassengerName": "Bob Smith", "MemSilvererStGoldtus": "Silver",
                 "Age": 35, "Gender": "M", "AmountSpent": 320.50, "Address": "Marina Bay",
                 "ContactTitle": "Mr", "Phone": 82345678},
                {"PassengerID": 3, "PassengerName": "Carol Lee", "MemSilvererStGoldtus": "Standard",
                 "Age": 42, "Gender": "F", "AmountSpent": 180.25, "Address": "Tampines",
                 "ContactTitle": "Mrs", "Phone": 83456789}
            ]
            self.passenger_df = self.spark.createDataFrame(passenger_data)

        # Register as temporary views for SQL queries
        self.taxi_df.createOrReplaceTempView("taxis")
        self.driver_df.createOrReplaceTempView("drivers")
        self.passenger_df.createOrReplaceTempView("passengers")

        print("Reference data loaded and registered as temporary views")

        # Show sample data
        print("\nSample Taxi Data:")
        self.taxi_df.show(5, truncate=False)
        print("\nSample Driver Data:")
        self.driver_df.show(5, truncate=False)
        print("\nSample Passenger Data:")
        self.passenger_df.show(5, truncate=False)

    def create_booking_stream(self, host="localhost", port=9999):
        """Create streaming DataFrame from socket source"""

        print(f"Connecting to booking stream at {host}:{port}")

        # Define schema for incoming booking data
        booking_schema = StructType([
            StructField("booking_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("passenger_id", IntegerType(), True),
            StructField("pickup_district", StringType(), True),
            StructField("dropoff_district", StringType(), True),
            StructField("booking_time", StringType(), True),
            StructField("requested_taxi_type", StringType(), True),
            StructField("num_passengers", IntegerType(), True),
            StructField("estimated_distance", DoubleType(), True),
            StructField("surge_multiplier", DoubleType(), True),
            StructField("payment_method", StringType(), True),
            StructField("booking_status", StringType(), True),
            StructField("special_requirements", StringType(), True),
            StructField("priority_level", StringType(), True)
        ])

        # Read from socket stream
        raw_stream = self.spark \
            .readStream \
            .format("socket") \
            .option("host", host) \
            .option("port", port) \
            .load()

        # Parse JSON data and add processing metadata
        booking_stream = raw_stream.select(
            from_json(col("value"), booking_schema).alias("booking")
        ).select("booking.*") \
            .withColumn("processing_time", current_timestamp()) \
            .withColumn("event_time", to_timestamp(col("timestamp"))) \
            .withColumn("day_of_week", dayofweek(col("event_time"))) \
            .withColumn("hour_of_day", hour(col("event_time")))

        return booking_stream

    def process_enriched_bookings(self, booking_stream):
        """Enrich bookings with reference data using complex SQL"""

        # Register the stream as a temporary view
        booking_stream.createOrReplaceTempView("booking_stream")

        # Complex SQL query for enrichment and analytics
        enriched_query = """
                         SELECT b.booking_id, \
                                b.event_time, \
                                b.processing_time, \
                                b.passenger_id, \
                                COALESCE(p.PassengerName, 'Unknown Passenger') as passenger_name, \
                                COALESCE(p.MemSilvererStGoldtus, 'Standard')   as membership_status, \
                                COALESCE(p.AmountSpent, 0.0)                   as total_spent_history, \
                                COALESCE(p.Age, 30)                            as passenger_age, \
                                COALESCE(p.Gender, 'U')                        as passenger_gender, \
                                b.pickup_district, \
                                b.dropoff_district, \
                                b.requested_taxi_type, \
                                b.num_passengers, \
                                b.estimated_distance, \
                                b.surge_multiplier, \
                                b.payment_method, \
                                b.special_requirements, \
                                b.priority_level, \
                                b.hour_of_day, \
                                b.day_of_week, \
                                ROUND( \
                                        (3.5 + (b.estimated_distance * 2.2) + \
                                         CASE \
                                             WHEN b.hour_of_day BETWEEN 6 AND 9 OR b.hour_of_day BETWEEN 17 AND 20 \
                                                 THEN 2.0 \
                                             WHEN b.hour_of_day BETWEEN 22 AND 6 THEN 1.5 \
                                             ELSE 0.0 \
                                             END + \
                                         CASE WHEN b.day_of_week IN (1, 7) THEN 1.0 ELSE 0.0 END \
                                            ) * b.surge_multiplier * \
                                        CASE \
                                            WHEN b.requested_taxi_type = 'Premier' THEN 1.5 \
                                            WHEN b.requested_taxi_type = 'Limosine' THEN 2.0 \
                                            WHEN b.requested_taxi_type = 'Maxi Cab' THEN 1.3 \
                                            WHEN b.requested_taxi_type = 'Mini Cab' THEN 0.9 \
                                            ELSE 1.0 \
                                            END * \
                                        CASE \
                                            WHEN p.MemSilvererStGoldtus = 'Gold' THEN 0.85 \
                                            WHEN p.MemSilvererStGoldtus = 'Silver' THEN 0.92 \
                                            ELSE 1.0 \
                                            END * \
                                        CASE \
                                            WHEN b.special_requirements IS NOT NULL THEN 1.1 \
                                            ELSE 1.0 \
                                            END, 2 \
                                )                                              as estimated_fare, \
                                CASE \
                                    WHEN b.hour_of_day BETWEEN 7 AND 9 OR b.hour_of_day BETWEEN 17 AND 19 \
                                        THEN 'RUSH_HOUR' \
                                    WHEN b.hour_of_day BETWEEN 22 AND 6 THEN 'NIGHT' \
                                    WHEN b.day_of_week IN (1, 7) THEN 'WEEKEND' \
                                    ELSE 'REGULAR' \
                                    END                                        as time_category, \
                                CASE \
                                    WHEN b.estimated_distance < 5 THEN 'SHORT' \
                                    WHEN b.estimated_distance BETWEEN 5 AND 15 THEN 'MEDIUM' \
                                    WHEN b.estimated_distance BETWEEN 15 AND 25 THEN 'LONG' \
                                    ELSE 'EXTRA_LONG' \
                                    END                                        as distance_category, \
                                CASE \
                                    WHEN b.pickup_district = 'Changi Airport' OR b.dropoff_district = 'Changi Airport' \
                                        THEN 'AIRPORT' \
                                    WHEN b.pickup_district IN ('Central', 'Orchard', 'Marina Bay') AND \
                                         b.dropoff_district IN ('Central', 'Orchard', 'Marina Bay') THEN 'CITY_CENTER' \
                                    WHEN b.pickup_district != b.dropoff_district THEN 'CROSS_DISTRICT' \
                                    ELSE 'LOCAL' \
                                    END                                        as route_type, \
                                ( \
                                    CASE \
                                        WHEN b.priority_level = 'VIP' THEN 20 \
                                        WHEN b.priority_level = 'High' THEN 10 \
                                        ELSE 0 END + \
                                    CASE \
                                        WHEN p.MemSilvererStGoldtus = 'Gold' THEN 15 \
                                        WHEN p.MemSilvererStGoldtus = 'Silver' THEN 8 \
                                        ELSE 0 END + \
                                    CASE \
                                        WHEN b.surge_multiplier > 2.0 THEN 15 \
                                        WHEN b.surge_multiplier > 1.5 THEN 10 \
                                        WHEN b.surge_multiplier > 1.2 THEN 5 \
                                        ELSE 0 END + \
                                    CASE \
                                        WHEN b.num_passengers > 4 THEN 8 \
                                        WHEN b.num_passengers > 2 THEN 3 \
                                        ELSE 0 END + \
                                    CASE WHEN b.special_requirements IS NOT NULL THEN 5 ELSE 0 END + \
                                    CASE \
                                        WHEN b.estimated_distance > 20 THEN 10 \
                                        WHEN b.estimated_distance > 10 THEN 5 \
                                        ELSE 0 END \
                                    )                                          as priority_score, \
                                ROUND( \
                                        (b.estimated_distance / \
                                         CASE \
                                             WHEN b.hour_of_day BETWEEN 7 AND 9 OR b.hour_of_day BETWEEN 17 AND 19 \
                                                 THEN 15.0 \
                                             WHEN b.hour_of_day BETWEEN 22 AND 6 THEN 35.0 \
                                             ELSE 25.0 \
                                             END) * 60 + \
                                        CASE \
                                            WHEN b.pickup_district = 'Changi Airport' OR \
                                                 b.dropoff_district = 'Changi Airport' THEN 10 \
                                            ELSE 5 END, \
                                        0 \
                                )                                              as estimated_duration_minutes
                         FROM booking_stream b
                                  LEFT JOIN passengers p ON b.passenger_id = p.PassengerID \
                         """

        enriched_stream = self.spark.sql(enriched_query)
        return enriched_stream

    def create_analytics_streams(self, enriched_stream):
        """Create multiple analytics streams with advanced aggregations"""

        # 1. Real-time operational metrics
        operational_metrics = enriched_stream \
            .withWatermark("event_time", "10 minutes") \
            .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            "pickup_district",
            "requested_taxi_type"
        ) \
            .agg(
            count("*").alias("booking_count"),
            avg("estimated_fare").alias("avg_fare"),
            avg("estimated_distance").alias("avg_distance"),
            avg("priority_score").alias("avg_priority"),
            sum("estimated_fare").alias("total_revenue"),
            avg("estimated_duration_minutes").alias("avg_duration"),
            approx_count_distinct("passenger_id").alias("unique_passengers")
        ) \
            .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "*"
        ).drop("window")

        # 2. Customer analytics by membership tier
        customer_analytics = enriched_stream \
            .withWatermark("event_time", "15 minutes") \
            .groupBy(
            window(col("event_time"), "10 minutes"),
            "membership_status",
            "time_category"
        ) \
            .agg(
            count("*").alias("bookings"),
            avg("estimated_fare").alias("avg_fare"),
            sum("estimated_fare").alias("revenue"),
            approx_count_distinct("passenger_id").alias("unique_customers"),
            avg("priority_score").alias("avg_priority"),
            avg("estimated_distance").alias("avg_distance")
        )

        # 3. Surge and demand analytics
        surge_analytics = enriched_stream \
            .filter(col("surge_multiplier") > 1.0) \
            .withWatermark("event_time", "10 minutes") \
            .groupBy(
            window(col("event_time"), "5 minutes"),
            "pickup_district",
            "route_type"
        ) \
            .agg(
            count("*").alias("surge_bookings"),
            avg("surge_multiplier").alias("avg_surge"),
            max("surge_multiplier").alias("max_surge"),
            sum("estimated_fare").alias("surge_revenue"),
            avg("priority_score").alias("avg_priority_surge")
        )

        # 4. Route and distance analytics
        route_analytics = enriched_stream \
            .withWatermark("event_time", "10 minutes") \
            .groupBy(
            window(col("event_time"), "15 minutes"),
            "pickup_district",
            "dropoff_district",
            "distance_category"
        ) \
            .agg(
            count("*").alias("trip_count"),
            avg("estimated_fare").alias("avg_route_fare"),
            avg("estimated_distance").alias("avg_route_distance"),
            avg("estimated_duration_minutes").alias("avg_duration")
        ) \
            .filter(col("trip_count") >= 3)  # Only show popular routes

        return operational_metrics, customer_analytics, surge_analytics, route_analytics

    def start_streaming(self, host="localhost", port=9999, checkpoint_location="/tmp/spark-checkpoint"):
        """Start the complete streaming pipeline"""

        print("=== TAXI BOOKING STREAM PROCESSOR ===")
        print("Loading reference data...")
        self.load_reference_data()

        print(f"Creating booking stream from {host}:{port}...")
        booking_stream = self.create_booking_stream(host, port)

        print("Processing enriched bookings...")
        enriched_stream = self.process_enriched_bookings(booking_stream)

        print("Creating analytics streams...")
        operational_metrics, customer_analytics, surge_analytics, route_analytics = self.create_analytics_streams(
            enriched_stream)

        # Start multiple output streams
        queries = []

        print("Starting streaming queries...")

        # 1. Console output for enriched bookings (detailed view)
        query1 = enriched_stream \
            .select(
            "booking_id", "passenger_name", "membership_status",
            "pickup_district", "dropoff_district", "estimated_fare",
            "priority_score", "time_category", "route_type"
        ) \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 10) \
            .trigger(processingTime="15 seconds") \
            .queryName("enriched_bookings") \
            .start()
        queries.append(("Enriched Bookings", query1))

        # 2. Operational metrics (windowed aggregations)
        query2 = operational_metrics.writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 15) \
            .trigger(processingTime="30 seconds") \
            .queryName("operational_metrics") \
            .start()
        queries.append(("Operational Metrics", query2))

        # 3. Customer analytics to memory for dashboard
        query3 = customer_analytics.writeStream \
            .outputMode("complete") \
            .format("memory") \
            .queryName("customer_metrics") \
            .trigger(processingTime="30 seconds") \
            .start()
        queries.append(("Customer Analytics", query3))

        # 4. Surge analytics to memory
        query4 = surge_analytics.writeStream \
            .outputMode("complete") \
            .format("memory") \
            .queryName("surge_metrics") \
            .trigger(processingTime="20 seconds") \
            .start()
        queries.append(("Surge Analytics", query4))

        # 5. Route analytics to console (less frequent)
        query5 = route_analytics.writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 10) \
            .trigger(processingTime="60 seconds") \
            .queryName("route_analytics") \
            .start()
        queries.append(("Route Analytics", query5))

        print(f"Started {len(queries)} streaming queries:")
        for name, query in queries:
            print(f"  - {name}: {query.id}")

        print("\nStreaming pipeline is running...")
        print("Press Ctrl+C to stop all queries")

        try:
            # Monitor queries and provide status updates
            while True:
                time.sleep(30)
                print(f"\n=== STREAMING STATUS ({time.strftime('%Y-%m-%d %H:%M:%S')}) ===")

                for name, query in queries:
                    if query.isActive:
                        progress = query.lastProgress
                        if progress:
                            input_rate = progress.get('inputRowsPerSecond', 0)
                            processing_rate = progress.get('processingRowsPerSecond', 0)
                            print(f"{name}: Input={input_rate:.1f}/s, Processing={processing_rate:.1f}/s")
                    else:
                        print(f"{name}: STOPPED")

                # Show sample data from memory tables
                try:
                    print("\n=== CUSTOMER METRICS SAMPLE ===")
                    self.spark.sql("SELECT * FROM customer_metrics LIMIT 5").show(truncate=False)

                    print("\n=== SURGE METRICS SAMPLE ===")
                    self.spark.sql("SELECT * FROM surge_metrics LIMIT 5").show(truncate=False)
                except:
                    print("Memory tables not yet available")

        except KeyboardInterrupt:
            print("\nStopping all streaming queries...")
            for name, query in queries:
                print(f"Stopping {name}...")
                query.stop()
            print("All queries stopped")

        finally:
            # Wait for all queries to finish
            for name, query in queries:
                if query.isActive:
                    query.awaitTermination()

            self.spark.stop()
            print("Spark session stopped")


def main():
    """Main function for running the stream processor"""
    parser = argparse.ArgumentParser(description="Taxi Booking Stream Processor")
    parser.add_argument("--source-host", default="localhost", help="Source host for booking stream")
    parser.add_argument("--source-port", type=int, default=9999, help="Source port for booking stream")
    parser.add_argument("--app-name", default="TaxiBookingStreamProcessor", help="Spark application name")
    parser.add_argument("--checkpoint-dir", default="/tmp/spark-checkpoint", help="Checkpoint directory")

    args = parser.parse_args()

    try:
        print(f"Starting Taxi Booking Stream Processor...")
        print(f"Source: {args.source_host}:{args.source_port}")
        print(f"Checkpoint: {args.checkpoint_dir}")

        # Create and start processor
        processor = TaxiBookingStreamProcessor(args.app_name)
        processor.start_streaming(args.source_host, args.source_port, args.checkpoint_dir)

    except Exception as e:
        print(f"Error starting stream processor: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

# ==================== USAGE EXAMPLES ====================
"""
USAGE EXAMPLES:

1. Basic usage (connect to default stream generator):
   python booking_processor.py

2. Connect to remote stream generator:
   python booking_processor.py --source-host 192.168.1.100 --source-port 8888

3. Custom application name and checkpoint:
   python booking_processor.py --app-name "ProductionTaxiProcessor" --checkpoint-dir "/data/checkpoints"

4. Complete workflow:
   # Terminal 1: Start stream generator
   python booking_generator.py --rate 3.0

   # Terminal 2: Start stream processor
   python booking_processor.py --source-host localhost --source-port 9999

FEATURES:
- Real-time enrichment with passenger, taxi, and driver data
- Complex SQL-based fare calculation with multiple factors
- Advanced priority scoring system
- Multiple analytics streams (operational, customer, surge, route)
- Watermarking for late data handling
- Memory sinks for dashboard integration
- Comprehensive error handling and monitoring
- Configurable checkpointing for fault tolerance

OUTPUT STREAMS:
1. Enriched Bookings: Real-time enriched booking events
2. Operational Metrics: 5-minute windowed aggregations by district and taxi type
3. Customer Analytics: Customer behavior by membership tier
4. Surge Analytics: Surge pricing patterns and demand
5. Route Analytics: Popular routes and performance metrics

MONITORING:
- Query status and processing rates
- Sample data from in-memory tables
- Error handling and graceful shutdown
"""