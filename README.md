# ETL Pipeline — MongoDB

A clean, modular **Extract → Transform → Load** pipeline written in Python that ingests data from external sources, applies configurable transformations, and loads the results into **MongoDB**. The entire stack is containerised with **Docker / Docker Compose** and ships with structured logging and robust error handling out of the box.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Running Locally](#running-locally)
  - [Running with Docker](#running-with-docker)
- [Pipeline Stages](#pipeline-stages)
  - [Ingest](#ingest)
  - [Transform](#transform)
  - [Load](#load)
- [Logging](#logging)
- [Error Handling](#error-handling)
- [Environment Variables](#environment-variables)
- [Contributing](#contributing)
- [License](#license)

---

## Features

| Feature | Details |
|---|---|
| **Modular design** | Each stage (Ingest, Transform, Load) lives in its own module and can be extended or swapped independently |
| **MongoDB sink** | Bulk-upserts transformed documents into a MongoDB collection with configurable write concern |
| **Structured logging** | JSON-formatted log lines with timestamps, log levels, and stage context via Python's `logging` module |
| **Error handling** | Per-record exception catching with retry logic; fatal errors surface with full tracebacks and stop the run cleanly |
| **Docker containerisation** | Multi-stage `Dockerfile` + `docker-compose.yml` for one-command startup of the pipeline and a local MongoDB instance |
| **Environment-based config** | All secrets and tunables live in a `.env` file — no hard-coded credentials |

---

## Architecture

```
┌──────────────┐     raw records      ┌─────────────────┐     clean docs      ┌────────────────┐
│   Data Source │ ──────────────────► │   Transform      │ ──────────────────► │    MongoDB     │
│  (Ingestor)   │                     │   (Transformer)  │                     │   (Loader)     │
└──────────────┘                     └─────────────────┘                     └────────────────┘
        │                                     │                                        │
        └─────────────────────────────────────┴────────────────────────────────────────┘
                                       Orchestrator (main.py)
                                       + Logger + Error Handler
```

---

## Project Structure

```
etl-pipline/
├── src/
│   ├── __init__.py
│   ├── ingest/
│   │   ├── __init__.py
│   │   └── ingestor.py        # Pulls raw data from the source
│   ├── transform/
│   │   ├── __init__.py
│   │   └── transformer.py     # Cleans and reshapes records
│   ├── load/
│   │   ├── __init__.py
│   │   └── loader.py          # Upserts documents into MongoDB
│   └── utils/
│       ├── __init__.py
│       ├── logger.py          # Centralized logging configuration
│       └── config.py          # Reads env vars / config file
├── tests/
│   ├── test_ingestor.py
│   ├── test_transformer.py
│   └── test_loader.py
├── .env.example               # Template for environment variables
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── main.py                    # Pipeline entry point
└── README.md
```

---

## Prerequisites

| Requirement | Minimum Version |
|---|---|
| Python | 3.10+ |
| Docker | 24+ |
| Docker Compose | v2 |
| MongoDB | 6.0+ (provided by Docker Compose) |

---

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/krush-ghod/etl-pipline.git
cd etl-pipline
```

### 2. Create and activate a virtual environment (local runs only)

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Set up environment variables

```bash
cp .env.example .env
# Edit .env with your preferred editor and fill in the required values
```

---

## Configuration

Copy `.env.example` to `.env` and adjust the values:

```dotenv
# ── Source ────────────────────────────────────────────────────────────────────
SOURCE_URL=https://example.com/api/data   # URL or file path to ingest data from
SOURCE_API_KEY=your_api_key_here

# ── MongoDB ───────────────────────────────────────────────────────────────────
MONGO_URI=mongodb://mongo:27017           # Use "localhost:27017" for local runs
MONGO_DB=etl_db
MONGO_COLLECTION=records

# ── Pipeline ──────────────────────────────────────────────────────────────────
BATCH_SIZE=500                            # Number of records processed per batch
LOG_LEVEL=INFO                            # DEBUG | INFO | WARNING | ERROR
```

---

## Usage

### Running Locally

```bash
# Ensure MongoDB is running and .env is configured
python main.py
```

To run a specific stage only:

```bash
python main.py --stage ingest      # Ingest only
python main.py --stage transform   # Transform only
python main.py --stage load        # Load only
```

### Running with Docker

**Start everything (pipeline + MongoDB) with a single command:**

```bash
docker compose up --build
```

**Run in detached mode:**

```bash
docker compose up -d --build
```

**View pipeline logs:**

```bash
docker compose logs -f etl
```

**Stop and remove containers:**

```bash
docker compose down
```

**Stop and also remove the MongoDB volume:**

```bash
docker compose down -v
```

---

## Pipeline Stages

### Ingest

`src/ingest/ingestor.py`

Responsible for pulling raw data from the configured source. Supports:

- **HTTP/REST APIs** — authenticated GET requests with pagination support
- **CSV / JSON files** — local or remote (S3-compatible) file reads
- Yields records in streaming batches to keep memory usage flat

### Transform

`src/transform/transformer.py`

Applies a configurable series of transformations to each raw record:

- Field renaming and type casting
- Null / missing-value handling
- Date normalisation to ISO-8601 UTC
- Custom business-logic hooks (extend `BaseTransformer`)

### Load

`src/load/loader.py`

Bulk-writes cleaned documents to MongoDB using `pymongo`'s `bulk_write` with `UpdateOne` + `upsert=True` semantics so re-runs are idempotent.

- Configurable upsert key field (default: `_id`)
- Ordered vs. unordered bulk operations
- Write-concern and read-preference settings exposed via environment variables

---

## Logging

Logging is configured centrally in `src/utils/logger.py` and shared across all modules.

- **Format:** JSON lines — easy to ingest into log aggregators (Loki, ELK, CloudWatch)
- **Levels:** Controlled via the `LOG_LEVEL` environment variable
- **Output:** `stdout` (captured by Docker) and optionally a rotating file handler
- Each log line includes: `timestamp`, `level`, `module`, `stage`, `message`, and any structured `extra` fields passed at the call site

Example log output:

```json
{"timestamp": "2026-03-09T02:00:00Z", "level": "INFO",  "stage": "ingest",    "message": "Fetched 500 records from source"}
{"timestamp": "2026-03-09T02:00:01Z", "level": "INFO",  "stage": "transform", "message": "Transformed 500 records (0 errors)"}
{"timestamp": "2026-03-09T02:00:02Z", "level": "INFO",  "stage": "load",      "message": "Upserted 500 documents into etl_db.records"}
```

---

## Error Handling

The pipeline handles errors at two levels:

| Level | Behaviour |
|---|---|
| **Record level** | Bad records are caught, logged with full context (`record_id`, `error`, `traceback`), and skipped — the batch continues |
| **Stage / fatal level** | Unrecoverable errors (e.g. MongoDB unreachable, source unavailable) raise exceptions that are logged and cause a non-zero exit code |

Transient network failures on ingest and load are retried automatically with **exponential back-off** (default: 3 retries, base delay 2 s).

---

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `SOURCE_URL` | ✅ | — | URL or path of the data source |
| `SOURCE_API_KEY` | — | — | API key for authenticated sources |
| `MONGO_URI` | ✅ | — | MongoDB connection string |
| `MONGO_DB` | ✅ | — | Target database name |
| `MONGO_COLLECTION` | ✅ | — | Target collection name |
| `BATCH_SIZE` | — | `500` | Records per processing batch |
| `LOG_LEVEL` | — | `INFO` | Logging verbosity |
| `MAX_RETRIES` | — | `3` | Max retry attempts for transient failures |
| `RETRY_DELAY` | — | `2` | Base delay in seconds for retry back-off |

---

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Commit your changes: `git commit -m "feat: add my feature"`
4. Push to the branch: `git push origin feature/my-feature`
5. Open a pull request

Please ensure all new code is covered by tests and passes the linter (`flake8` / `ruff`).

---

## License

This project is licensed under the [MIT License](LICENSE).
