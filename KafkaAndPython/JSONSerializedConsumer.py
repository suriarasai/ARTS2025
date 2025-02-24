from kafka import KafkaConsumer
import json

# Kafka docker URL
broker = "localhost:9092"
topic = "json-test-topic"


# Function to create a Kafka consumer
def create_consumer(broker, topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[broker],
        auto_offset_reset='earliest',  # Read messages from the beginning of the topic
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # Deserialize bytes into JSON
    )
    return consumer


# Function to consume messages
def consume_messages(consumer):
    try:
        for message in consumer:
            print(f"Received message: {message.value} from topic: {message.topic}")
    except KeyboardInterrupt:
        print("Stopped consuming messages.")
    finally:
        consumer.close()


if __name__ == "__main__":
    consumer = create_consumer(broker, topic)
    consume_messages(consumer)
