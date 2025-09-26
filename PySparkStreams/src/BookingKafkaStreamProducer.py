from kafka import KafkaProducer
import time
import json

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092']  # Replace with your Kafka broker(s)
)

# File path to read
file_path = 'onefilebooking/Booking.json'

# Kafka topic to which the events will be sent
topic_name = 'booking'


def read_json_objects(file_path, producer, topic_name):
    with open(file_path, 'r') as file:
        for line in file:
            try:
                json_object = json.loads(line.strip())
                print(json_object)  # Process the JSON object as needed
                producer.send(topic_name, json.dumps(json_object).encode('utf-8'))
                producer.flush()
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                time.sleep(1)

if __name__ == '__main__':
    try:
        # Start reading the file and streaming to Kafka
        read_json_objects(file_path, producer, topic_name)
    except KeyboardInterrupt:
        print('Stopping file streaming...')
    finally:
        # Close the Kafka producer when done
        producer.close()
