# hello_producer.py
# Day 3 – Simple Kafka producer proof-of-life (prints only – no real connection yet)

from kafka import KafkaProducer
import json

print("Day 3: kafka-python imported successfully!")
print("KafkaProducer class ready:", KafkaProducer)

# Simulate a sample market event (we'll send real ones later)
sample_event = {
    "ticker": "AAPL",
    "price": 195.25,
    "timestamp": 1737570000000,
    "volume": 150
}

print("\nSample event to send (JSON):")
print(json.dumps(sample_event, indent=2))

print("\nIn the next step, we'll configure bootstrap_servers and actually send this to Kafka.")
print("Run me with: python src/hello_producer.py")