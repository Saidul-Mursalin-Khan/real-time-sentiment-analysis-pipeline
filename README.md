# Realtime Reddit Sentiment Analysis

A cloud-deployed, real-time sentiment analysis pipeline built on Apache Kafka, Apache Flink, and PostgreSQL. The system streams Reddit comments, preprocesses and classifies them using a self-trained TF-IDF + Logistic Regression model. The model automatically retrains as more data is processed.

## Architecture

```
                                     RC_2019-04.zst
                                          │
                                          ▼
┌───────────────────────────── Data Ingestion Layer ─────────────────────────────┐
│   ┌───────────────┐          ┌───────────────────────┐                          │
│   │ data-provider │ ───────▶ │        Kafka          │                          │
│   │ (producer.py) │          │ topic: reddit-comments│                          │
│   └───────────────┘          └───────────┬───────────┘                          │
└──────────────────────────────────────────┼──────────────────────────────────────┘

                                            ▼
┌───────────────────── Stream Processing Layer — Apache Flink ───────────────────┐
│   ┌────────────────────┐     ┌──────────────────┐     ┌──────────────────┐     │
│   │  preprocess-flink  │────▶│  labeling-flink  │────▶│ inference-flink  │     │
│   │ (preprocess_job.py)│     │(labeling_job.py) │     │ (inference.py)   │     │
│   └────────────────────┘     └──────────────────┘     └────────┬─────────┘     │
└────────────────────────────────────────────────────────────────┼───────────────┘

                                                                  ▼
┌──────────────────────────────── ML Model Layer ────────────────────────────────┐
│   ┌───────────────┐        ┌──────────────────────┐    ┌──────────────────┐    │
│   │ train-consumer│───────▶│  TF-IDF + LogReg     │───▶│sentiment_model_vN│    │
│   │  (train.py)   │        │  (auto retrain)      │    └──────────────────┘    │
│   └───────────────┘        └──────────────────────┘                            │
└────────────────────────────────────────────────────────────────────────────────┘

                                                                  ▼
┌──────────────────────── Storage & Presentation Layer ──────────────────────────┐
│   ┌──────────────┐    ┌────────────────┐    ┌──────────────┐    ┌──────────┐   │
│   │  PostgreSQL  │    │ FastAPI backend│───▶│   Frontend   │    │ Kafka UI │   │
│   │  (results)   │    │   (app.py)     │    │  (static/)   │    │          │   │
│   └──────────────┘    └────────────────┘    └──────────────┘    └──────────┘   │
└────────────────────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
bd26_project_f6_a/
├── data-provider/
│   ├── Dockerfile
│   ├── producer.py           # Reads RC_2019-04.zst, replays to Kafka by timestamp
│   └── requirements.txt
├── dataset/
│   └── RC_2019-04.zst        # Reddit comments dataset (not in repo — download separately)
├── preprocess-flink/
│   ├── Dockerfile
│   ├── preprocess_job.py     # Flink job: cleans raw comments → cleaned-comments topic
│   └── requirements.txt
├── labeling-flink/
│   ├── Dockerfile
│   ├── labeling_job.py       # Flink job: labels cleaned comments → labeled-train-data topic
│   └── requirements.txt
├── inference-flink/
│   ├── Dockerfile
│   ├── inference.py          # Flink job: runs sentiment inference using trained model
│   └── requirements.txt
├── train-consumer/
│   ├── Dockerfile
│   ├── train.py              # Consumes labeled data, trains TF-IDF + LogReg model
│   ├── data/
│   │   └── oracle_dataset_clean.csv
│   └── requirements.txt
├── backend/
│   ├── Dockerfile
│   ├── app.py                # FastAPI backend — REST API + Kafka producer/consumer
│   ├── static/               # Frontend HTML/JS/CSS
│   └── requirements.txt
├── docker-compose.yml
├── .env
└── README.md
```

## Prerequisites

- Docker Engine + Docker Compose v2
- `RC_2019-04.zst` dataset placed in `./dataset/` directory
  - Download from: [The Pushshift Reddit Dataset](https://files.pushshift.io/reddit/comments/)
  - Only `RC_2019-04.zst` (comments file) is needed — not the submissions file

## Quick Start

### 1. Clone the repository

```bash
git clone https://collaborating.tuhh.de/e-19/teaching/bd26_project_f6_a.git
cd bd26_project_f6_a
git checkout dev
```

### 2. Configure environment

```bash
cp .env.example .env   # or create manually
```

Default `.env` contents:
```env
KEYWORDS=python,java
SPEED_FACTOR=100
RETRAIN_EVERY=50000
MAX_SAMPLES=300000
DB_HOST=postgres
DB_PORT=5432
DB_NAME=sentiment
DB_USER=sentiment
DB_PASS=sentiment
```

### 3. Place the dataset

```
bd26_project_f6_a/
└── dataset/
    └── RC_2019-04.zst   ← place file here
```

### 4. Build all images

```bash
docker compose build
```

### 5. Start infrastructure

```bash
docker compose up -d zookeeper kafka postgres pgadmin kafka-ui
```

Wait for all services to be healthy:
```bash
docker compose ps
# zookeeper, kafka, postgres should show "healthy"
```

### 6. Initialize Kafka topics

```bash
docker compose up kafka-init
```

Wait for: `Topics created and configs synced.`

### 7. Start all processing services

```bash
docker compose up -d preprocess-flink labeling-flink inference-flink trainer backend
```

### 8. Stream Reddit data into Kafka

```bash
docker compose up data-provider
```

Monitor progress:
```bash
docker logs -f data-provider
```

---

## Verify Data is Flowing

```bash
# Check sentiment results in PostgreSQL
docker exec postgres psql -U sentiment -d sentiment \
  -c "SELECT sentiment, count(*) FROM sentiment_results GROUP BY sentiment;"

# Check Kafka topic message counts
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server kafka:9092 \
  --topic reddit-comments
```

---

## Cloud Deployment (GCP)

The solution is deployed on Google Cloud Platform (europe-west3 / Frankfurt).

| Service | Public URL |
|---|---|
| Frontend + API | http://35.198.116.145:8000 |
| API Docs (Swagger) | http://35.198.116.145:8000/docs |
| Kafka UI | http://35.198.116.145:8080 |
| pgAdmin | http://35.198.116.145:5050 |

**VM Specs:** `e2-standard-4` (4 vCPU, 16GB RAM), Ubuntu 22.04 LTS, europe-west3-a

---

## Services

| Service | Description | Port |
|---|---|---|
| `zookeeper` | Kafka coordination | 2181 |
| `kafka` | Message broker | 29092 (host) / 9092 (internal) |
| `kafka-init` | Creates all required Kafka topics | — |
| `kafka-ui` | Kafka web UI | 8080 |
| `postgres` | Sentiment results storage | 5432 |
| `pgadmin` | PostgreSQL web UI | 5050 |
| `data-provider` | Kafka producer — replays .zst file | — |
| `preprocess-flink` | Flink job: cleans raw comments | — |
| `labeling-flink` | Flink job: labels comments for training | — |
| `inference-flink` | Flink job: runs sentiment inference | — |
| `trainer` | Trains/retrains TF-IDF + LogReg model | — |
| `backend` | FastAPI REST API + frontend | 8000 |

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Service health check |
| GET | `/model/status` | Latest trained model info |
| POST | `/watch` | Register keywords for monitoring |
| GET | `/series` | Sentiment history for visualization |
| POST | `/analyze` | One-time sentiment analysis on custom input |
| GET | `/` | Serves the frontend dashboard |

---

## Configuration

Key environment variables (set in `.env`):

| Variable | Default | Description |
|---|---|---|
| `KEYWORDS` | `python,java` | Comma-separated keywords to track |
| `SPEED_FACTOR` | `100` | Kafka replay speed multiplier |
| `RETRAIN_EVERY` | `50000` | Trigger retraining every N rows |
| `MAX_SAMPLES` | `300000` | Max samples per training run |
| `DB_HOST` | `postgres` | PostgreSQL host |
| `DB_NAME` | `sentiment` | PostgreSQL database name |
| `DB_USER` | `sentiment` | PostgreSQL username |
| `DB_PASS` | `sentiment` | PostgreSQL password |

---

## Docker Hub Images

```bash
docker pull bd26_project_f6_a-backend:latest
docker pull bd26_project_f6_a-data-provider:latest
docker pull bd26_project_f6_a-inference-flink:latest
docker pull bd26_project_f6_a-labeling-flink:latest
docker pull bd26_project_f6_a-preprocess-flink:latest
docker pull bd26_project_f6_a-trainer:latest
```

---

## Reset Everything (Fresh Start)

```bash
# Stop all containers and delete all volumes
docker compose down -v

# Free up unused Docker resources
docker system prune -f
```

---

## Kafka Topics

| Topic | Description | Partitions |
|---|---|---|
| `reddit-comments` | Raw comments from data-provider | 8 |
| `cleaned-comments` | Preprocessed comments | 8 |
| `labeled-train-data` | Labeled data for model training | 4 |
| `model-version` | Model version update notifications | 1 |
| `inference-request` | Inference requests from backend | 4 |
| `inference-response` | Inference results to backend | 4 |

---

## Team

- **Repository**: https://collaborating.tuhh.de/e-19/teaching/bd26_project_f6_a
- **Branch**: `dev`
- **Course**: Big Data — Project, TUHH
- **Topic**: A — Sentiment Analysis