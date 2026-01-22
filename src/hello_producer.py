# hello_producer.py
# Day 7 – Add basic anomaly detection: flag price jumps >5% from last sent

from kafka import KafkaProducer
import json
import time
import random

# Local Kafka settings
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'market-quotes'

print("Day 7: Starting fake market producer with anomaly detection...")
print(f"Sending simulated updates to topic: {TOPIC_NAME}")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

tickers = ['AAPL', 'TSLA', 'GOOGL']
last_prices = {ticker: 150.0 for ticker in tickers}  # Track last price per ticker

try:
    while True:
        for ticker in tickers:
            # Simulate price change
            base_price = last_prices[ticker] + random.uniform(-3, 3)
            price = round(base_price + random.uniform(-2, 2), 2)

            event = {
                "ticker": ticker,
                "price": price,
                "timestamp": int(time.time() * 1000),
                "volume": random.randint(50, 500)
            }

            # Simple anomaly rule: >5% change from last sent price
            change_pct = abs((price - last_prices[ticker]) / last_prices[ticker]) * 100
            if change_pct > 5:
                print(f"ANOMALY DETECTED! {ticker} price jump: {change_pct:.1f}% ({last_prices[ticker]:.2f} → {price:.2f})")

            last_prices[ticker] = price  # Update last price

            print(f"Sending: {event}")
            producer.send(TOPIC_NAME, value=event)
            producer.flush()

        time.sleep(3)  # Send every 3 seconds

except KeyboardInterrupt:
    print("\nStopped by user (Ctrl+C)")

finally:
    producer.close()
    print("Producer closed – goodbye!")