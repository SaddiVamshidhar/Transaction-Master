# Transaction Master Service

This is a production-founded prototype of a microservice responsible for processing algorithmic trading transactions. It ingests raw trade data via a webhook, enriches it, persists it idempotently to a PostgreSQL database, and publishes the enriched data to a Kafka topic for downstream consumers.

## Features

- **FastAPI**: High-performance asynchronous web framework.
- **Kafka Integration**: Asynchronous messaging with `aiokafka` for robust data pipelines.
- **PostgreSQL Backend**: Reliable and transactional data storage using `SQLAlchemy` (async) and `asyncpg`.
- **Idempotent Processing**: Ensures that duplicate messages are handled gracefully without creating duplicate records.
- **Observability**: Structured JSON logging, Prometheus metrics (`/metrics`), and health checks (`/health`).
- **Containerized**: Fully containerized with Docker and Docker Compose for easy local development and deployment.
- **Migrations**: Database schema migrations managed by `Alembic`.
- **Testing**: Comprehensive unit tests with `pytest`.

## Getting Started

### Prerequisites

- Docker
- Docker Compose

### 1. Setup Environment

First, copy the example environment file. The default values are configured to work with the provided `docker-compose.yml`.

```bash
cp .env.example .env