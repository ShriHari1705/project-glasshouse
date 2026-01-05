# 🏛️ Glasshouse: Real-Time Crypto Sentiment Pipeline

An end-to-end data engineering pipeline that correlates Binance trade firehoses with AI-driven social sentiment analysis.

## 🚀 Key Achievements
- **Efficient LLM Deployment**: Reduced Docker build context from **1.6GB to <5MB** using bind mounts and advanced `.dockerignore` strategies.
- **Robust Stream Processing**: Implemented a decoupled architecture using **Redpanda** (Kafka-compatible) to handle high-frequency data spikes.
- **OLAP Optimized Storage**: Built a high-concurrency schema in **ClickHouse** to store and query 1M+ trade records with sub-second latency.

## 🧠 Tech Stack
- **Streaming**: Redpanda
- **AI/ML**: RoBERTa (NLP) via HuggingFace Transformers
- **Database**: ClickHouse (OLAP)
- **UI**: Streamlit for real-time visualization

## 🛠️ Lessons Learned
1. **Startup Race Conditions**: Solved container initialization issues using Docker health checks and Python retry loops.
2. **Type Consistency**: Managed strict `DateTime64` formatting requirements in ClickHouse using `python-dateutil`.
