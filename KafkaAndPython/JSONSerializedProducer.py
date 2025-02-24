import json
from kafka import KafkaProducer

# Kafka docker URL
broker = "localhost:9092"

# Function to create a Kafka producer
def create_producer(broker):
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda m: json.dumps(m).encode('utf-8')  # Serialize JSON messages to bytes
    )
    return producer


# Function to send messages
def send_message(producer, topic, message):
    try:
        # Send message to Kafka
        future = producer.send(topic, message)
        result = future.get(timeout=10)  # Block until a single message is sent (or timeout)
        print(f"Message sent: {message} to topic: {result.topic}, partition: {result.partition}")
    except Exception as e:
        print(f"An error occurred: {e}")



# Example usage
producer = create_producer(broker)
send_message(producer, 'json-test-topic', {'id': 1, 'name': 'Tan Ah Beng'})

