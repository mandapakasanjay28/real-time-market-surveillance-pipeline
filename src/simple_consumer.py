# simple_consumer.py
# Day 13 – Add velocity-based anomaly detection (change over last 3 messages)

from kafka import KafkaConsumer
import json
from collections import deque  # For fixed-size history per ticker

BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'market-quotes'
HISTORY_LENGTH = 3  # Check change over last 3 prices

print("Day 13: Starting multi-ticker consumer with velocity-based anomaly detection...")
print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}")
print(f"Listening to topic: {TOPIC_NAME}")

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='day13-surveillance-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

price_history = {}  # Dict of ticker → deque of last N prices
anomaly_count = 0

print("Consumer ready – waiting for messages...")

for message in consumer:
    event = message.value
    ticker = event['ticker']
    price = event['price']

    print(f"Received: {event}")

    # Initialize history for new ticker
    if ticker not in price_history:
        price_history[ticker] = deque(maxlen=HISTORY_LENGTH)

    # Add current price to history
    price_history[ticker].append(price)

    # Velocity anomaly rule: only check if we have enough history
    if len(price_history[ticker]) == HISTORY_LENGTH:
        prices = list(price_history[ticker])
        total_change = abs(prices[-1] - prices[0]) / prices[0] * 100
        if total_change > 5:
            anomaly_count += 1
            print(f"ANOMALY DETECTED! {ticker} velocity jump over {HISTORY_LENGTH} messages: {total_change:.1f}% ({prices[0]:.2f} → {prices[-1]:.2f})")
            print(f"Total anomalies detected so far: {anomaly_count}")

    print("-" * 50)

    # Stop after 50 messages for this test (remove for continuous)
    if sum(len(h) for h in price_history.values()) > 50:
        break

consumer.close()
print(f"Consumer closed – test complete.")
print(f"Final anomaly count: {anomaly_count}")
print("Run me with: python src/simple_consumer.py")