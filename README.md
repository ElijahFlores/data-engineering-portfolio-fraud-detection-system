# Real-Time Fraud Detection System

> A production-grade streaming data pipeline for detecting fraudulent financial transactions in near real-time

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![Spark](https://img.shields.io/badge/Spark-3.5-orange.svg)](https://spark.apache.org/)

## Overview

This system processes financial transactions in real-time using Apache Kafka and Spark Structured Streaming, applying rule-based fraud detection algorithms to flag suspicious activities within seconds of occurrence.

**Key Metrics:**
- **< 5 second detection latency**
- **1000+ transactions/second throughput**
- **94%+ detection accuracy (recall)**
- **< 3% false positive rate**

## Architecture
```
[Transaction Generator] → [Kafka] → [Spark Streaming] → [Fraud Detection Engine]
                                                              ↓
                                          ┌─────────────────┬─────────────┬──────────┐
                                          ↓                 ↓             ↓          ↓
                                    [PostgreSQL]       [Redis]      [Alerts]   [Grafana]
                                   (Persistence)    (Real-time)     (Slack)   (Dashboard)
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| **Streaming Platform** | Apache Kafka 7.5 |
| **Stream Processing** | Apache Spark 3.5 (Structured Streaming) |
| **Data Warehouse** | PostgreSQL 15 |
| **Cache Layer** | Redis 7 |
| **API** | FastAPI + Uvicorn |
| **Monitoring** | Grafana 10 | [WIP!]
| **Infrastructure** | Docker Compose |
| **Language** | Python 3.9+ |

## Features

### Fraud Detection Engine
- **Multi-rule Detection:**
  - High-value transactions (>$5,000)
  - Suspicious geographic locations
  - Untrusted merchants
  - Round-amount patterns
  - Rapid transaction bursts (3+ in 10 seconds)

- **Scoring System:** 0-100 fraud risk score with weighted rules
- **Real-time Alerts:** Automated Slack/email notifications
- **Low Latency:** Sub-5-second detection pipeline

### Analytics & Monitoring
- **REST API** with 8+ endpoints for fraud analytics
- **Live Grafana Dashboards** with 5-second refresh [WIP]
- **Detection Metrics:** Precision, recall, accuracy tracking
- **User Risk Profiles:** Identify high-risk accounts

### Production-Ready Design
- **Fault Tolerance:** Kafka message persistence + Spark checkpointing
- **Scalability:** Horizontally scalable architecture
- **Observability:** Comprehensive logging and metrics
- **Data Quality:** Schema validation and error handling

## Quick Start

### Prerequisites
- Docker Desktop
- Python 3.9+
- Java 11 (for Spark)
- 8GB RAM minimum

### Installation

1. **Clone repository:**
```bash
git clone https://github.com/yourusername/fraud-detection-system.git
cd fraud-detection-system
```

2. **Start infrastructure:**
```bash
docker-compose up -d
sleep 30  # Wait for Kafka initialization
```

3. **Install Python dependencies:**
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

4. **Run pipeline (4 terminals):**

Terminal 1:
```bash
python data_generator/simulate_transactions.py
```

Terminal 2:
```bash
python spark_streaming/fraud_processor.py
```

Terminal 3:
```bash
python alerts/alert_system.py
```

Terminal 4:
```bash
cd api && uvicorn dashboard_api:app --reload --port 8000
```

5. **Access dashboards:**
- **API Docs:** http://localhost:8000/docs
- **Grafana:** http://localhost:3000 (admin/admin)

## Performance Benchmarks

Tested on MacBook Pro M1, 16GB RAM:

| Metric | Value |
|--------|-------|
| Processing Latency (p95) | 3.2 seconds |
| Throughput | 1,247 TPS |
| Detection Recall | 94.3% |
| Precision | 96.8% |
| False Positive Rate | 2.1% |
| API Response Time (p95) | 87ms |

## API Examples
```bash
# Get real-time stats
curl http://localhost:8000/stats/realtime

# Query recent frauds
curl http://localhost:8000/frauds/recent?limit=10

# Detection metrics
curl http://localhost:8000/metrics/detection
```

## Skills Demonstrated

- ✅ Event-driven architecture design
- ✅ Real-time stream processing at scale
- ✅ Anomaly detection algorithms
- ✅ Database optimization (indexes, partitioning)
- ✅ RESTful API development
- ✅ Docker containerization
- ✅ Production observability (logging, monitoring)
- ✅ Data pipeline orchestration

## Future Enhancements

- [ ] Grafana Dashboard
- [ ] ML-based anomaly detection (Isolation Forest)
- [ ] Apache Flink alternative implementation
- [ ] AWS deployment (MSK + Lambda + RDS)
- [ ] Kubernetes orchestration
- [ ] Data quality checks (Great Expectations)
- [ ] AB testing framework for detection rules

## Project Structure
```
fraud-detection-system/
├── data_generator/          # Transaction simulator
├── spark_streaming/         # Fraud detection processor
├── alerts/                  # Alert system (Slack/Email)
├── api/                     # FastAPI backend
├── sql/                     # Database schemas
├── notebooks/               # Analysis notebooks
├── docker-compose.yml       # Infrastructure definition
└── README.md
```

## Contact

**Elijah Flores**  
Email: altonflores@yahoo.com  
LinkedIn: [linkedin.com/in/yourprofile](https://www.linkedin.com/in/elijahflores/)  
GitHub: [@yourusername](https://github.com/ElijahFlores)

---

⭐ If this project helped you, please star it on GitHub!