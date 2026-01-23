# simple_consumer.py
# Day 12 – Add anomaly count & basic logging to multi-ticker consumer

from kafka import KafkaConsumer
import json

BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'market-quotes'

print("Day 12: Starting multi-ticker consumer with anomaly detection & logging...")
print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}")
print(f"Listening to topic: {TOPIC_NAME}")

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='day12-surveillance-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

last_prices = {}  # Track last price per ticker
anomaly_count = 0  # Total anomalies detected

print("Consumer ready – waiting for messages...")

for message in consumer:
    event = message.value
    ticker = event['ticker']
    price = event['price']

    print(f"Received: {event}")

    # Anomaly rule: >5% change from last seen price for this ticker
    if ticker in last_prices:
        change_pct = abs((price - last_prices[ticker]) / last_prices[ticker]) * 100
        if change_pct > 5:
            anomaly_count += 1
            print(f"ANOMALY DETECTED! {ticker} price jump: {change_pct:.1f}% ({last_prices[ticker]:.2f} → {price:.2f})")
            print(f"Total anomalies detected so far: {anomaly_count}")
    else:
        print(f"First price seen for {ticker}")

    last_prices[ticker] = price  # Update last price

    print("-" * 50)

    # Stop after 30 messages for this test (remove for continuous listening)
    if len(last_prices) > 30:
        break

consumer.close()
print(f"Consumer closed – test complete.")
print(f"Final anomaly count: {anomaly_count}")
print("Run me with: python src/simple_consumer.py")

