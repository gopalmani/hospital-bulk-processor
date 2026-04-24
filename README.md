# Hospital Bulk Processing System

FastAPI service for uploading a CSV of hospitals, creating them through the deployed Hospital Directory API, and activating the batch only after all rows are created successfully.

## Features

- `POST /hospitals/bulk` accepts a CSV upload and starts an async background job.
- `GET /jobs/{batch_id}` returns job status, progress, row-level results, activation state, and processing time.
- `GET /jobs` lists all known in-memory jobs.
- CSV validation requires `name,address`, allows optional `phone`, ignores empty rows, and limits uploads to 20 hospitals.
- External API calls retry transient failures up to 3 times with `1s`, `2s`, and `4s` backoff.
- Hospital creation is limited to 5 concurrent external API calls.
- If any row fails permanently, the service rolls back the batch using `DELETE /hospitals/batch/{batch_id}`.
- Structured logs are emitted for job acceptance, row success/failure, retries, activation, rollback, and job completion.

## CSV Example

```csv
name,address,phone
ABC Hospital,123 Main Street,555-1001
City Care Clinic,45 Park Avenue,555-1002
Sunrise Medical Center,88 Lake Road,
```

## Run Locally

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/uvicorn app.main:app --reload
```

API docs:

```text
http://localhost:8000/docs
```

## Test

```bash
.venv/bin/python -m pytest -q
```

## API Examples

Start a bulk job:

```bash
curl -X POST http://localhost:8000/hospitals/bulk \
  -F "file=@hospitals.csv"
```

Check one job:

```bash
curl http://localhost:8000/jobs/{batch_id}
```

List jobs:

```bash
curl http://localhost:8000/jobs
```

## Deploy To Render

Use a Render Web Service with:

```text
Runtime: Python 3
Build Command: pip install -r requirements.txt
Start Command: uvicorn app.main:app --host 0.0.0.0 --port $PORT
```

If deploying with Docker, the included Dockerfile also respects Render's `PORT` environment variable.

## Design Note

This implementation intentionally returns `202 Accepted` immediately and exposes progress through polling endpoints. That is more production-friendly than blocking the upload request until every external API call finishes. In-memory storage is used as allowed by the assignment; in production, this would move to Redis or Postgres.
