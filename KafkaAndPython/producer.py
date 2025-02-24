from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

for i in range(10):
    message = f"Hello Kafka {i}"
    producer.send('my-topic', message.encode())
    time.sleep(1)

producer.flush()