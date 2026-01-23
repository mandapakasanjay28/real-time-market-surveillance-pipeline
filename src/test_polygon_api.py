# test_polygon_api.py
# Day 9 – Test real Polygon.io API call with local .env key

import os
from dotenv import load_dotenv
import requests
import json

load_dotenv()  # Loads .env file

API_KEY = os.getenv('POLYGON_API_KEY')
TICKER = 'AAPL'  # Test with Apple stock

if not API_KEY:
    print("ERROR: POLYGON_API_KEY not found in .env file")
else:
    print("API key loaded successfully (starts with:", API_KEY[:5], "... )")

    url = f"https://api.polygon.io/v3/trades/{TICKER}?limit=1&apiKey={API_KEY}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        print("\nSuccessful API response:")
        print(json.dumps(data, indent=2))
        print("\nLast trade price for", TICKER, ":", data['results']['p'])
    else:
        print("API error:", response.status_code)
        print(response.text)