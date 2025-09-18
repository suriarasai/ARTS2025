from kafka import KafkaProducer
import json
import time

# Initialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Adjust if your Kafka server is on a different host/port
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Sample JSON records
data = [
    {"CreatedTime": "2025-02-25 10:12:00", "Type": "BUY", "Amount": 300, "BrokerCode": "SGX"},
    {"CreatedTime": "2025-02-25 10:05:00", "Type": "BUY", "Amount": 500, "BrokerCode": "SGX"},
    {"CreatedTime": "2025-02-25 10:20:00", "Type": "BUY", "Amount": 800, "BrokerCode": "SGX"},
    {"CreatedTime": "2025-02-25 10:40:00", "Type": "BUY", "Amount": 900, "BrokerCode": "SGX"},
    {"CreatedTime": "2025-02-25 10:25:00", "Type": "SELL", "Amount": 400, "BrokerCode": "SGX"},
    {"CreatedTime": "2025-02-25 10:48:00", "Type": "SELL", "Amount": 600, "BrokerCode": "SGX"},
    {"CreatedTime": "2025-02-25 11:05:00", "Type": "SELL", "Amount": 200, "BrokerCode": "SGX"},
    {"CreatedTime": "2025-02-25 10:12:00", "Type": "SELL", "Amount": 200, "BrokerCode": "SGX"}
]

# Send each record to the 'stock' topic
for record in data:
    producer.send("stock", value=record)
    print(f"Sent: {record}")
    time.sleep(1)  # Optional: sleep to simulate streaming

# Ensure all messages are sent before closing the producer
producer.flush()
producer.close()
