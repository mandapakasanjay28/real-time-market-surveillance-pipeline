# hello_producer.py
# Day 9 – Use real AAPL price from yfinance instead of random

from kafka import KafkaProducer
import json
import time
import yfinance as yf

# Local Kafka settings
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'market-quotes'

print("Day 9: Starting producer with real AAPL data from yfinance...")
print(f"Sending updates to topic: {TOPIC_NAME}")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

ticker_symbol = "AAPL"

try:
    while True:
        # Fetch real current price from yfinance
        stock = yf.Ticker(ticker_symbol)
        price = stock.info.get('regularMarketPrice', 0.0)  # fallback to 0 if error

        if price == 0:
            print("Warning: Could not fetch real price – skipping")
            time.sleep(3)
            continue

        event = {
            "ticker": ticker_symbol,
            "price": price,
            "timestamp": int(time.time() * 1000),
            "volume": stock.info.get('regularMarketVolume', 0)
        }

        print(f"Sending real AAPL data: {event}")
        producer.send(TOPIC_NAME, value=event)
        producer.flush()

        time.sleep(3)  # Update every 3 seconds (respect yfinance rate limits)

except KeyboardInterrupt:
    print("\nStopped by user (Ctrl+C)")

finally:
    producer.close()
    print("Producer closed – goodbye!")