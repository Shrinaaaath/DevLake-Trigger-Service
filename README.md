# DevLake Trigger Service

A lightweight event-driven service that receives GitHub repository webhook events, validates the request signature, publishes the repository activity to Kafka, deduplicates repeated events, resolves the repository to one or more DevLake blueprint IDs, and triggers the corresponding DevLake pipeline(s).

## What this project does

- Accepts GitHub webhook requests through FastAPI
- Verifies the `X-Hub-Signature-256` header using HMAC-SHA256
- Normalizes the repository identifier to `owner/repo`
- Publishes repository events to Kafka
- Deduplicates repeated events in a short time window
- Resolves repository-to-blueprint mappings
- Triggers DevLake blueprint execution via the DevLake REST API

## Architecture summary

GitHub webhook -> FastAPI -> signature verification -> Kafka producer -> Kafka consumer -> deduplication -> blueprint resolution -> DevLake trigger

This is a working prototype / proof of concept, not a full production-grade distributed orchestration platform.

## Tech stack

- Python 3.12
- FastAPI
- Pydantic + Pydantic Settings
- Kafka (`aiokafka`)
- `httpx` for DevLake API calls
- Apache DevLake v1.0.2
- MySQL for the DevLake backend

## Local setup

1. Create and activate a virtual environment.
2. Install Python dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Start the local supporting services:

   ```bash
   docker compose up -d
   ```

4. Start the application:

   ```bash
   PYTHONPATH=. uvicorn app.main:app --host 0.0.0.0 --port 8000
   ```

## Health and webhook endpoints

- `GET /health`
- `GET /readiness`
- `POST /webhook`

## Tests

Run the repository test suite with:

```bash
PYTHONPATH=. pytest -q
```

## Project documentation

Detailed architecture, assumptions, runtime evidence, and handover notes are in:

- [PROJECT_DOCUMENTATION.md](PROJECT_DOCUMENTATION.md)

## Database

The DevLake backend uses MySQL.

## Notes

- Deduplication and blueprint cache are currently in-memory only.
- Blueprint locking is process-local and suitable for prototype use.
- This project is designed to validate the pattern and operational flow, not to replace a multi-node enterprise deployment.
