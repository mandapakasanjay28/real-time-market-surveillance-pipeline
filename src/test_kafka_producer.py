# test_kafka_producer.py
# Day 4 – Test local Kafka connection and send one message

from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import json
import time

# Local Kafka settings from docker-compose.yml
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'test-topic-day4'

print("Day 4: Testing Kafka connection...")

# Step 1: Create admin client to create topic if not exists
admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
topic_list = admin_client.list_topics()

if TOPIC_NAME not in topic_list:
    print(f"Creating topic: {TOPIC_NAME}")
    new_topic = NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)
    admin_client.create_topics(new_topics=[new_topic])
    time.sleep(2)  # Give Kafka a moment to create the topic
else:
    print(f"Topic {TOPIC_NAME} already exists")

# Step 2: Producer to send a message
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sample_message = {
    "event": "Day 4 test",
    "ticker": "TEST",
    "price": 100.0,
    "timestamp": int(time.time() * 1000)
}

print("Sending sample message...")
producer.send(TOPIC_NAME, value=sample_message)
producer.flush()  # Wait for send to complete

print("Message sent successfully!")
print("Sample message:", json.dumps(sample_message, indent=2))

producer.close()
print("\nTest complete – Kafka connection works!")
print("Run me with: python src/test_kafka_producer.py")