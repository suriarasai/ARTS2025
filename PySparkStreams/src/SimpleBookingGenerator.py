from kafka import KafkaProducer
import json
import time
import random

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# List of locations
locations = ['Downtown', 'Airport', 'Mall', 'Train Station', 'University']

def generate_booking():
    return {
        "booking_id": str(random.randint(1000, 9999)),
        "customer_id": str(random.randint(1000, 9999)),
        "pickup_location": random.choice(locations),
        "dropoff_location": random.choice(locations),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

# Produce booking events
while True:
    booking = generate_booking()
    producer.send('taxi_bookings', booking)
    print(f"Produced: {booking}")
    time.sleep(1)  # Produce a new booking every second
