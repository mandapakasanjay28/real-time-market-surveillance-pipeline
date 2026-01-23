# hello_producer.py
# Day 10 – Multi-ticker real market data from yfinance (AAPL, TSLA, GOOGL)

from kafka import KafkaProducer
import json
import time
import yfinance as yf

# Local Kafka settings
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'market-quotes'

print("Day 10: Starting multi-ticker producer with real data from yfinance...")
print(f"Sending updates to topic: {TOPIC_NAME}")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

tickers = ['AAPL', 'TSLA', 'GOOGL']

try:
    while True:
        for ticker_symbol in tickers:
            # Fetch real current price from yfinance
            stock = yf.Ticker(ticker_symbol)
            price = stock.info.get('regularMarketPrice', 0.0)

            if price == 0:
                print(f"Warning: Could not fetch price for {ticker_symbol} – skipping")
                continue

            event = {
                "ticker": ticker_symbol,
                "price": price,
                "timestamp": int(time.time() * 1000),
                "volume": stock.info.get('regularMarketVolume', 0)
            }

            print(f"Sending real data for {ticker_symbol}: {event}")
            producer.send(TOPIC_NAME, value=event)
            producer.flush()

        time.sleep(5)  # Slightly longer delay for multiple tickers (rate limit friendly)

except KeyboardInterrupt:
    print("\nStopped by user (Ctrl+C)")

except Exception as e:
    print(f"Unexpected error: {e}")

finally:
    producer.close()
    print("Producer closed – goodbye!")