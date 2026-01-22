# hello_producer.py
# Day 6 – Evolve producer: send fake market updates in loop (simulated prices)

from kafka import KafkaProducer
import json
import time
import random

# Local Kafka settings
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'market-quotes'

print("Day 6: Starting fake market producer...")
print(f"Sending simulated updates to topic: {TOPIC_NAME}")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

tickers = ['AAPL', 'TSLA', 'GOOGL']

try:
    while True:  # Loop forever – Ctrl+C to stop
        for ticker in tickers:
            # Simulate price change: base + random fluctuation
            base_price = 150 + random.uniform(-10, 10)  # random between 140–160
            price = round(base_price + random.uniform(-2, 2), 2)  # small change

            event = {
                "ticker": ticker,
                "price": price,
                "timestamp": int(time.time() * 1000),
                "volume": random.randint(50, 500)
            }

            print(f"Sending: {event}")
            producer.send(TOPIC_NAME, value=event)
            producer.flush()  # Ensure sent

        time.sleep(3)  # Send every 3 seconds

except KeyboardInterrupt:
    print("\nStopped by user (Ctrl+C)")

finally:
    producer.close()
    print("Producer closed – goodbye!")