from kafka import KafkaConsumer
import avro.schema
from avro.io import DatumReader, BinaryDecoder
import io

# Load Avro schema
schema_path = "user.avsc"
schema = avro.schema.parse(open(schema_path).read())

consumer = KafkaConsumer(
    'users',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest'
)


def decode_message(message):
    bytes_reader = io.BytesIO(message)
    decoder = BinaryDecoder(bytes_reader)
    reader = DatumReader(schema)
    return reader.read(decoder)


for msg in consumer:
    user_record = decode_message(msg.value)
    print(f"Received: {user_record}")
