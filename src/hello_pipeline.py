# hello_pipeline.py
# Day 2 proof-of-life script – confirms venv, packages, and basic setup

import requests
from dotenv import load_dotenv
import os

# Just to prove dotenv works (we'll use real .env later)
load_dotenv()

print("Day 2: Setup successful!")
print(f"requests version: {requests.__version__}")
print(f"Current working directory: {os.getcwd()}")
print("venv active and packages ready for Polygon API ingestion next.")

if __name__ == "__main__":
    print("\nRun me with: python src/hello_pipeline.py")