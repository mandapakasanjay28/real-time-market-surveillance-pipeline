# Real-Time Market Surveillance & Anomaly Detection Pipeline

**Production-grade streaming data engineering project**  
Built to demonstrate end-to-end real-time data engineering skills for top-tier roles (NVIDIA, Amazon, Fidelity).

**Business value**  
Ingests live market quotes → detects price/volume anomalies in near real-time → enables rapid alerting for risk, fraud, or market surveillance.

**Tech stack (2026 modern production flavor)**  
- Ingestion: Polygon.io API → Apache Kafka  
- Processing: Apache Flink (PyFlink) – stateful windows, exactly-once semantics, watermarks for late events  
- Storage: Medallion lakehouse (bronze/raw → silver/cleaned → gold/aggregated) with Delta Lake on MinIO/S3  
- Observability: Evidently AI (data drift/quality), Grafana dashboards & alerts, Prometheus metrics  
- Orchestration: Airflow/Dagster (planned for scheduling & backfills)

**Current status**  
Day 1 – Foundation: Clean repo structure, GitHub setup, professional README.

**Goal**  
Build a resilient, observable streaming pipeline that handles high-velocity data, schema evolution, and production failures — exactly the kind of work that powers financial systems at scale.

Proudly built in Fuquay-Varina, NC  
Targeting $200k+ Data Engineering roles

Work in progress – daily commits & updates.

## Day 5 Progress (Kafka Consumer Loop)
- Added `simple_consumer.py`: Connects to local Kafka, reads from `test-topic-day4` from earliest offset  
- Verified full round-trip: producer → Kafka → consumer prints the same JSON message  
- Confirmed bidirectional flow works end-to-end  
- Committed & pushed consumer script


## Day 10 Progress (Multi-Ticker Real Data Streaming)
- Updated `hello_producer.py`: Fetch real prices for AAPL, TSLA, GOOGL from yfinance  
- Producer sends diversified live market events every 5 seconds to `market-quotes` topic  
- Ran producer → saw actual current prices printing and streaming  
- Committed & pushed multi-ticker real-data version


## Day 11 Progress (Multi-Ticker Consumer Anomaly Detection)
- Updated `simple_consumer.py`: Track last price per ticker (AAPL, TSLA, GOOGL, etc.)  
- Added anomaly rule: Flag >5% price jumps on incoming real events  
- Ran producer + consumer side-by-side → saw live multi-ticker prices + anomalies flagged in real time  
- Committed & pushed enhanced consumer script

## Day 12 Progress (Anomaly Logging & Count in Consumer)
- Enhanced `simple_consumer.py`: Added anomaly count tracking & running logging  
- Prints "Total anomalies detected so far: X" after each flag  
- Final summary "Final anomaly count: X" at close  
- Ran consumer with real multi-ticker data → saw live logging & count in action  
- Committed & pushed updated consumer script