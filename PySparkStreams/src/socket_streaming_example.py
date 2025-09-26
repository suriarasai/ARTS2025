#!/usr/bin/env python3
"""
PySpark 3.5 LTS Socket Streaming Example with Kafka Integration
This script demonstrates socket communication using ncat and Kafka streaming.

Usage:
    # Terminal 1: Start ncat listener
    nc -lk 9999

    # Terminal 2: Run this script
    python socket_streaming_example.py

    # Terminal 3: Send data to socket
    echo "Hello Spark!" | nc localhost 9999
"""

import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import sys
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session(app_name="SocketStreamingExample"):
    """Create and configure Spark session for streaming."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.streaming.checkpointLocation", "/app/checkpoints") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def socket_streaming_example():
    """Example of reading from socket stream."""
    logger.info("Starting Socket Streaming Example...")

    spark = create_spark_session("SocketStreamingExample")

    try:
        # Read from socket stream
        socket_df = spark \
            .readStream \
            .format("socket") \
            .option("host", "localhost") \
            .option("port", 9999) \
            .option("includeTimestamp", True) \
            .load()

        # Process the stream
        processed_df = socket_df.select(
            col("value").alias("message"),
            col("timestamp"),
            length(col("value")).alias("message_length"),
            when(col("value").contains("error"), "ERROR")
            .when(col("value").contains("warn"), "WARNING")
            .otherwise("INFO").alias("log_level")
        )

        # Write to console
        query = processed_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime='5 seconds') \
            .start()

        logger.info("Socket streaming started. Send messages to 'nc localhost 9999'")
        logger.info("Spark UI available at: http://localhost:4040")

        # Keep the stream running
        query.awaitTermination(60)  # Run for 60 seconds or until manually stopped

    except Exception as e:
        logger.error(f"Error in socket streaming: {e}")
    finally:
        spark.stop()


def kafka_streaming_example():
    """Example of Kafka streaming integration."""
    logger.info("Starting Kafka Streaming Example...")

    spark = create_spark_session("KafkaStreamingExample")

    try:
        # Read from Kafka
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "test-topic") \
            .option("startingOffsets", "latest") \
            .load()

        # Process Kafka messages
        processed_df = kafka_df.select(
            col("key").cast("string"),
            col("value").cast("string").alias("message"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp")
        ).withColumn("processing_time", current_timestamp())

        # Write processed data back to Kafka (different topic)
        query = processed_df.select(
            to_json(struct("*")).alias("value")
        ).writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", "processed-topic") \
            .option("checkpointLocation", "/app/checkpoints/kafka") \
            .outputMode("append") \
            .start()

        logger.info("Kafka streaming started.")
        logger.info("Producing to 'processed-topic', consuming from 'test-topic'")

        # Also write to console for monitoring
        console_query = processed_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime='10 seconds') \
            .start()

        # Keep both streams running
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Error in Kafka streaming: {e}")
    finally:
        spark.stop()


def socket_to_kafka_pipeline():
    """Complete pipeline: Socket -> Processing -> Kafka."""
    logger.info("Starting Socket-to-Kafka Pipeline...")

    spark = create_spark_session("SocketToKafkaPipeline")

    try:
        # Read from socket
        socket_df = spark \
            .readStream \
            .format("socket") \
            .option("host", "localhost") \
            .option("port", 9999) \
            .load()

        # Process and enrich the data
        enriched_df = socket_df.select(
            col("value").alias("original_message"),
            current_timestamp().alias("ingestion_time"),
            lit("socket").alias("source"),
            regexp_extract(col("value"), r"(\d+)", 1).alias("extracted_number"),
            when(col("value").rlike(r"\d+"), "contains_number")
            .otherwise("text_only").alias("message_type")
        ).withColumn(
            "message_id",
            concat(lit("msg_"), unix_timestamp(), lit("_"), rand())
        )

        # Convert to JSON for Kafka
        kafka_ready_df = enriched_df.select(
            col("message_id").alias("key"),
            to_json(struct("*")).alias("value")
        )

        # Write to Kafka
        kafka_query = kafka_ready_df.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", "socket-messages") \
            .option("checkpointLocation", "/app/checkpoints/socket-kafka") \
            .outputMode("append") \
            .start()

        # Also write to console for monitoring
        console_query = enriched_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime='5 seconds') \
            .start()

        logger.info("Pipeline started: Socket(9999) -> Processing -> Kafka(socket-messages)")
        logger.info("Send messages: echo 'Hello 123!' | nc localhost 9999")

        # Wait for termination
        kafka_query.awaitTermination()

    except Exception as e:
        logger.error(f"Error in pipeline: {e}")
    finally:
        spark.stop()


def test_ncat_connection():
    """Test ncat connection and setup."""
    import socket
    import time

    logger.info("Testing ncat connection...")

    try:
        # Test if port 9999 is open
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('localhost', 9999))
        sock.close()

        if result == 0:
            logger.info("✓ Port 9999 is open and ready for connections")
            return True
        else:
            logger.warning("✗ Port 9999 is not available")
            logger.info("Start ncat listener: nc -lk 9999")
            return False

    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return False


if __name__ == "__main__":
    print("PySpark 3.5 LTS Socket Streaming Examples")
    print("=" * 50)

    # Test connection first
    if not test_ncat_connection():
        print("\nTo start ncat listener:")
        print("1. In container: podman-compose exec pyspark-app nc -lk 9999")
        print("2. Or use make.bat: make ncat")
        print("3. Then run this script again")
        sys.exit(1)

    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
    else:
        print("\nAvailable modes:")
        print("1. socket    - Basic socket streaming")
        print("2. kafka     - Kafka streaming")
        print("3. pipeline  - Socket to Kafka pipeline")
        mode = input("\nSelect mode (socket/kafka/pipeline): ").strip().lower()

    try:
        if mode == "socket":
            socket_streaming_example()
        elif mode == "kafka":
            kafka_streaming_example()
        elif mode == "pipeline":
            socket_to_kafka_pipeline()
        else:
            print("Invalid mode. Use: socket, kafka, or pipeline")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Streaming stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        sys.exit(1)