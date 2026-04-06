<div align="center">

# 🛒 Instacart Real-Time Supply Chain Pipeline

**End-to-end streaming analytics pipeline for real-time retail demand prediction and stock optimization**

![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)
![Apache NiFi](https://img.shields.io/badge/Apache_NiFi-728E9B?style=for-the-badge&logo=apache&logoColor=white)
![ClickHouse](https://img.shields.io/badge/ClickHouse-FFCC01?style=for-the-badge&logo=clickhouse&logoColor=black)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)

[![Last Commit](https://img.shields.io/github/last-commit/ejabra/instacart-pipeline?color=green)](https://github.com/ejabra/instacart-pipeline)
[![Repo Size](https://img.shields.io/github/repo-size/ejabra/instacart-pipeline)](https://github.com/ejabra/instacart-pipeline)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

</div>

---

## 📌 Problem Statement

Traditional retail systems rely on **batch processing** — stock decisions are made on data that is hours old. This causes:
- **Overstock** → product waste and tied-up capital
- **Stockouts** → lost revenue and poor customer experience
- **No real-time visibility** into demand patterns

## 💡 Solution

A fully containerized **streaming data pipeline** that ingests Instacart order events in real time, applies ML-based demand forecasting, and surfaces live KPIs through interactive dashboards — enabling stock decisions within **seconds** of a transaction.

---

## 🏗️ Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌──────────────┐    ┌─────────────────────┐
│  Data Source │───▶│ Apache NiFi │───▶│ Apache Kafka│───▶│   Python     │───▶│     ClickHouse      │
│  (Instacart │    │  Ingestion  │    │  Streaming  │    │  Consumers   │    │   (OLAP Storage)    │
│   Dataset)  │    │  & Routing  │    │   Broker    │    │  + ML Model  │    │                     │
└─────────────┘    └─────────────┘    └─────────────┘    └──────────────┘    └──────────┬──────────┘
                                                                                          │
                                                                            ┌─────────────▼──────────┐
                                                                            │  Streamlit  │  Power BI │
                                                                            │   Dashboard │  Dashboard│
                                                                            └────────────────────────┘
```

| Layer | Component | Role |
|-------|-----------|------|
| Ingestion | Apache NiFi | Flow-based data routing, transformation, provenance tracking |
| Streaming | Apache Kafka | Message broker — high-throughput, fault-tolerant event log |
| Processing | Python + Pandas | Data cleaning, enrichment, feature engineering |
| ML | Scikit-Learn (Random Forest) | Demand prediction per product category |
| Storage | ClickHouse | Columnar OLAP — optimized for fast analytical queries |
| Visualization | Streamlit + Power BI | Real-time dashboards and business KPIs |
| Orchestration | Docker Compose | Full stack containerization (6 services) |
| Governance | Marquez / OpenLineage | Data lineage tracking |

![Architecture](/docs/screenshots/architecture.png)

---

## ⚡ Performance Results

| Metric | Result |
|--------|--------|
| Dataset size | 3M+ orders (Instacart 2017) |
| Ingestion throughput | ~12,000 events / second |
| ClickHouse query latency | < 200ms on 3M rows |
| vs MySQL on same aggregations | **10x faster** |
| End-to-end pipeline lag | < 5 seconds |
| ML prediction accuracy (R²) | 0.79 — Random Forest |
| Services in Docker Compose | 6 (NiFi, Kafka, Zookeeper, ClickHouse, MongoDB, Streamlit) |

---

## ✨ Key Features

- **Real-time ingestion** — NiFi processors ingest and route CSV/JSON order events into Kafka topics
- **Streaming processing** — Python consumers transform and enrich events before writing to ClickHouse
- **Demand forecasting** — Random Forest model predicts next-period demand by product category (R² = 0.79)
- **OLAP analytics** — ClickHouse enables sub-200ms queries on millions of rows
- **Live dashboards** — Streamlit and Power BI dashboards refresh in real time
- **Data lineage** — OpenLineage/Marquez tracks data origin and transformations
- **Fully containerized** — One `docker-compose up` launches the entire stack

---

## 📊 Dashboards

### Streamlit — Real-Time Monitoring
> Live pipeline metrics: throughput, consumer lag, top products, demand curve

![STREAMLIT DASHBOARD](/docs/screenshots/streamlit.png)

### Power BI — Business Intelligence
> Stock KPIs, demand heatmaps, category performance, prediction vs actual

![POWER BI DASHBOARD](/docs/screenshots/powerbi_1.png)

![POWER BI DASHBOARD](/docs/screenshots/powerbi_2.png)

*(Screenshots in `/docs/screenshots/` — see dashboard previews in the images above)*

---

## 🚀 Getting Started

### Prerequisites

- Docker & Docker Compose installed
- Python 3.9+
- Git
- Ports available: **8080** (NiFi), **9092** (Kafka), **8123** (ClickHouse), **27017** (MongoDB), **8501** (Streamlit)

### 1. Clone the repository

```bash
git clone https://github.com/ejabra/instacart-pipeline.git
cd instacart-pipeline
```

### 2. Launch the infrastructure

```bash
docker-compose up -d
```

Wait ~60 seconds for all services to initialize. Check status:

```bash
docker-compose ps
```

### 3. Run the data producer

```bash
python producer.py
```

This simulates real-time order events from the Instacart dataset and publishes them to Kafka.

### 4. Start the Streamlit dashboard

```bash
streamlit run streamlit_app/app.py
```

Open [http://localhost:8501](http://localhost:8501) to view the live dashboard.

### 5. Access services

| Service | URL | Credentials |
|---------|-----|-------------|
| Apache NiFi | http://localhost:8080/nifi | admin / admin |
| ClickHouse (HTTP) | http://localhost:8123 | default / (no password) |
| Streamlit Dashboard | http://localhost:8501 | — |
| Marquez UI | http://localhost:3000 | — |

---

## 🧠 Technology Decisions

**Why ClickHouse instead of PostgreSQL?**
Columnar storage makes analytical aggregations (GROUP BY, COUNT, SUM over millions of rows) 10–100x faster than row-based databases. ClickHouse is purpose-built for OLAP. For transactional writes, MongoDB handles document-style order storage.

**Why Kafka instead of a simple message queue (Redis, RabbitMQ)?**
Kafka's persistent, replayed log is critical for fault tolerance. If a consumer crashes, it can replay from the last committed offset without data loss. It also decouples producers from consumers completely, allowing independent scaling.

**Why NiFi for ingestion?**
300+ built-in processors, visual flow design, and built-in data provenance (full audit trail of every record). Ideal for prototyping complex routing logic — no custom ingestion code needed.

**Why Random Forest for demand prediction?**
Handles non-linear relationships between features (day of week, product category, historical velocity) well. Interpretable via feature importance. Scikit-learn implementation is fast enough for batch inference on the stream.

---

## 🔧 Challenges & Solutions

**1. Kafka consumer lag under high load**
When the producer ran at full speed, consumers fell behind. Fixed by tuning `max.poll.records=500` and adding a second consumer in the same consumer group, effectively parallelizing topic partition reads.

**2. NiFi backpressure causing flow stalls**
Processor queues filled up under burst traffic. Resolved by configuring `Back Pressure Object Threshold` on queues and adding a rate-limiting processor upstream to smooth ingestion spikes.

**3. ClickHouse schema evolution mid-pipeline**
Adding new analytical columns after the initial schema required a migration strategy without downtime. Used `ALTER TABLE ADD COLUMN IF NOT EXISTS` — ClickHouse fills new columns with default values for existing rows without rewriting the table.

**4. Docker memory pressure with 6 services**
Running NiFi + Kafka + ClickHouse + MongoDB simultaneously required ~6GB RAM. Resolved by setting JVM heap limits in `docker-compose.yml` (`NIFI_JVM_HEAP_MAX=1g`, `KAFKA_HEAP_OPTS=-Xmx512m`).

---

## 📁 Repository Structure

```
instacart-pipeline/
├── docker-compose.yml          # Full stack: NiFi, Kafka, ClickHouse, MongoDB, Streamlit, Marquez
├── producer.py                 # Kafka producer — simulates real-time order events
├── consumer.py                 # Kafka consumer — transforms and writes to ClickHouse
├── models/
│   └── demand_model.py         # Random Forest demand forecasting model
├── nifi/
│   └── instacart_flow.xml      # NiFi flow definition (export)
├── clickhouse/
│   └── schema.sql              # ClickHouse table definitions
├── streamlit_app/
│   └── app.py                  # Real-time Streamlit dashboard
├── powerbi/
│   └── instacart_dashboard.pbix # Power BI report file
├── docs/
│   ├── architecture.png        # Architecture diagram
│   └── screenshots/            # Dashboard screenshots
└── README.md
```

---

## 🗺️ Roadmap

- [ ] Replace Python consumers with **Apache Flink** for stateful stream processing
- [ ] Add **Apache Airflow** for orchestrating batch ML retraining
- [ ] Implement **schema registry** (Confluent) for Kafka message versioning
- [ ] Add **Grafana + Prometheus** for infrastructure monitoring
- [ ] Deploy to **AWS (MSK + ECS)** — cloud-native version
- [ ] Add **dbt** models on top of ClickHouse for semantic layer

---

## 👤 Author

**Brahim Dargui** — Data Engineer  
📍 Casablanca, Morocco  
[![LinkedIn](https://img.shields.io/badge/LinkedIn-dargui-blue?logo=linkedin)](https://linkedin.com/in/dargui)
[![GitHub](https://img.shields.io/badge/GitHub-ejabra-black?logo=github)](https://github.com/ejabra)
📧 brahimdargui@gmail.com

**Nouhaila BENNANI** - Data Engineer
📍 Casablanca, Morocco  

---

## 📄 License

This project is open source under the [MIT License](LICENSE).

---

<div align="center">
<i>Built as a PFE (Final Year Project) at Ynov Campus Casablanca — Data Engineering track, 2025–2026</i>
</div>
