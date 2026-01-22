# simple_consumer.py
# Day 5 – Simple Kafka consumer to read from test-topic-day4

from kafka import KafkaConsumer
import json

BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'test-topic-day4'

print("Day 5: Starting simple consumer...")
print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}")
print(f"Listening to topic: {TOPIC_NAME}")

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',  # Read from beginning
    enable_auto_commit=True,
    group_id='day5-test-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Consumer ready – waiting for messages...")

for message in consumer:
    print("\nReceived message:")
    print(f"Topic: {message.topic}")
    print(f"Partition: {message.partition}")
    print(f"Offset: {message.offset}")
    print(f"Value: {message.value}")
    print("-" * 50)

    # Stop after first message for this test (remove to keep listening)
    break

consumer.close()
print("Consumer closed – test complete.")
print("Run me with: python src/simple_consumer.py")