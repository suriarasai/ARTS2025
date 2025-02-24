from kafka import KafkaConsumer

topic_name = 'my-topic'
consumer = KafkaConsumer(topic_name,
                        auto_offset_reset='earliest',
                        bootstrap_servers=['localhost:9092'],
                        consumer_timeout_ms=10000)
l =[]
for msg in consumer:
    l.append(msg.value)
consumer.close()
print(l)