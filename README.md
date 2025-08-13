# OTX Pulses ETL Connector

**Connector:** otx_pulses_connector  
**Target:** AlienVault OTX (/api/v1/pulses/subscribed) → MongoDB

## What this script does
1. Reads OTX API key and DB settings from local `.env`.
2. Extracts subscribed pulses from OTX using the `X-OTX-API-KEY` header.
3. Transforms each pulse into a Mongo-friendly document and adds `ingestion_timestamp`.
4. Loads/upserts documents into `DB_NAME.COLLECTION_NAME`.

## Setup
1. Copy `.env.sample` → `.env` and fill values (do NOT commit `.env`).
2. Add `.env` to `.gitignore`.
3. Install deps:
pip install -r requirements.txt

markdown
Copy
Edit
4. Run:
python etl_connector.py

pgsql
Copy
Edit

## Notes & validation
- The OTX API expects `X-OTX-API-KEY` header for authentication. See OTX docs. :contentReference[oaicite:3]{index=3}
- Primary endpoint used: `GET /api/v1/pulses/subscribed`. You must have subscribed to at least one pulse for results. :contentReference[oaicite:4]{index=4}
- The connector respects basic rate-limits (handles 429 using `Retry-After` when present).
- Single Mongo collection per connector is used (`COLLECTION_NAME` env var). Each doc contains `ingestion_timestamp` for audits.

## Git & submission checklist
- Create a branch: `git checkout -b your-branch-name`
- Add files (do NOT add `.env`):
git add etl_connector.py requirements.txt README.md .env.sample
git commit -m "YourName RollNumber: add OTX pulses connector"
git push origin your-branch-name

pgsql
Copy
Edit
- Open PR and include your name & roll number in description and commit messages.

## Resources
- OTX API main site & docs (endpoints & examples). :contentReference[oaicite:5]{index=5}
- OTX Python SDK (optional): https://github.com/AlienVault-OTX/OTX-Python-SDK. :contentReference[oaicite:6]{i