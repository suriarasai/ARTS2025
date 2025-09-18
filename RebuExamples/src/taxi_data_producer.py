# src/taxi_data_producer.py
import json
import time
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaProducer
import random
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TaxiDataProducer:
    def __init__(self, kafka_servers='localhost:9092', topic_name='taxi-trips'):
        self.kafka_servers = kafka_servers
        self.topic_name = topic_name
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: str(x).encode('utf-8')
        )

    def load_taxi_data(self, csv_file_path):
        """Load taxi data from CSV file"""
        try:
            df = pd.read_csv(csv_file_path)
            logger.info(f"Loaded {len(df)} records from {csv_file_path}")
            return df
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return None

    def transform_to_streaming_event(self, row, base_time=None):
        """Transform CSV row to streaming event with current timestamp"""
        if base_time is None:
            base_time = datetime.now()

        # Add some randomness to simulate real-time streaming
        timestamp_offset = random.randint(-300, 300)  # ±5 minutes variation
        event_time = base_time + timedelta(seconds=timestamp_offset)

        # Create streaming event
        event = {
            'trip_id': str(row['Sno']),
            'event_time': event_time.isoformat(),
            'processing_time': datetime.now().isoformat(),
            'pickup_district': row['PickupDistrict'],
            'dropoff_district': row['DropOffDistrict'],
            'distance_km': float(row['DistanceTravelled']),
            'duration_seconds': int(row['TripDurationInSeconds']),
            'fare_amount': float(row['TripFare']),
            'taxi_type': row['TaxiType'],
            'taxi_capacity': int(row['TaxiCapacity']),
            'passenger_count': int(row['NumberOfPassengers']),
            'hour_of_day': int(row['HourOfDay']),
            'day_of_week': row['Day']
        }

        return event

    def stream_data(self, df, events_per_second=10, duration_minutes=30):
        """Stream taxi data to Kafka topic"""
        logger.info(f"Starting to stream data at {events_per_second} events/second for {duration_minutes} minutes")

        total_events = 0
        start_time = datetime.now()
        base_event_time = datetime.now()

        # Calculate sleep time between events
        sleep_time = 1.0 / events_per_second

        try:
            while (datetime.now() - start_time).total_seconds() < duration_minutes * 60:
                # Select random row from dataframe
                row = df.sample(n=1).iloc[0]

                # Create streaming event
                event = self.transform_to_streaming_event(row, base_event_time)

                # Send to Kafka
                future = self.producer.send(
                    self.topic_name,
                    key=event['trip_id'],
                    value=event
                )

                total_events += 1

                # Log progress every 100 events
                if total_events % 100 == 0:
                    logger.info(f"Sent {total_events} events to topic '{self.topic_name}'")

                # Advance base time to maintain chronological order
                base_event_time += timedelta(seconds=random.uniform(0.5, 2.0))

                time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Streaming interrupted by user")
        except Exception as e:
            logger.error(f"Error during streaming: {e}")
        finally:
            logger.info(f"Total events sent: {total_events}")
            self.producer.flush()
            self.producer.close()

    def simulate_late_data(self, df, late_events_count=50):
        """Generate late arriving data for watermarking demonstration"""
        logger.info(f"Generating {late_events_count} late-arriving events")

        current_time = datetime.now()

        for i in range(late_events_count):
            row = df.sample(n=1).iloc[0]

            # Create event with timestamp in the past (simulating late data)
            late_minutes = random.randint(5, 30)  # 5-30 minutes late
            event_time = current_time - timedelta(minutes=late_minutes)

            event = self.transform_to_streaming_event(row, event_time)
            event['is_late_data'] = True
            event['late_by_minutes'] = late_minutes

            future = self.producer.send(
                self.topic_name,
                key=f"late_{event['trip_id']}",
                value=event
            )

            time.sleep(0.1)  # Small delay between late events

        self.producer.flush()
        logger.info(f"Sent {late_events_count} late-arriving events")


if __name__ == "__main__":
    import sys
    import os

    # Default parameters
    csv_file = "/app/data/input/RebuTripData.csv"
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

    # Check if CSV file exists
    if not os.path.exists(csv_file):
        logger.error(f"CSV file not found: {csv_file}")
        logger.info("Please ensure the RebuTripData.csv file is in the /app/data directory")
        sys.exit(1)

    # Create producer
    producer = TaxiDataProducer(kafka_servers=kafka_servers)

    # Load data
    df = producer.load_taxi_data(csv_file)
    if df is None:
        sys.exit(1)

    # Parse command line arguments
    events_per_second = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    duration_minutes = int(sys.argv[2]) if len(sys.argv) > 2 else 30

    logger.info("🚕 TAXI DATA STREAMING PRODUCER 🚕")
    logger.info(f"Kafka Servers: {kafka_servers}")
    logger.info(f"Events per second: {events_per_second}")
    logger.info(f"Duration: {duration_minutes} minutes")

    # Start streaming
    try:
        producer.stream_data(df, events_per_second, duration_minutes)

        # Generate some late data for watermarking demo
        logger.info("Generating late data for watermarking demonstration...")
        time.sleep(2)
        producer.simulate_late_data(df, 20)

    except Exception as e:
        logger.error(f"Producer failed: {e}")
        sys.exit(1)