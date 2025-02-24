from kafka import KafkaProducer
from kafka.errors import KafkaError
import avro.schema
from avro.io import DatumWriter, BinaryEncoder
import io

# Load Avro schema
schema_path = "user.avsc"
schema = avro.schema.parse(open(schema_path).read())

producer = KafkaProducer(bootstrap_servers='localhost:9092')


def send_message(topic, message):
    writer = DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(message, encoder)
    raw_bytes = bytes_writer.getvalue()

    try:
        producer.send(topic, raw_bytes)
        producer.flush()
        print(f"Message sent to {topic}")
    except KafkaError as e:
        print(f"An error occurred: {e}")


# Example usage
send_message('users', {'id': 1, 'name': 'Tan Ah Beng', 'email': 'ah.beng@example.iss'})
