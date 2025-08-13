#!/usr/bin/env python3
"""
etl_connector.py
AlienVault OTX -> MongoDB ETL connector (pulses.subscribed example)

Usage:
  1. Create a local .env (see .env.sample) and fill values.
  2. pip install -r requirements.txt
  3. python etl_connector.py
"""

import os
import time
import logging
from datetime import datetime
from typing import Generator, Dict, Any, List, Optional

import requests
from pymongo import MongoClient, errors
from dotenv import load_dotenv

# -- load environment variables
load_dotenv()  # loads .env from current directory

OTX_API_KEY = os.getenv("OTX_API_KEY")
BASE_URL = os.getenv("BASE_URL", "https://otx.alienvault.com/api/v1")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "api_testing")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "otx_pulses_raw")
CONNECTOR_NAME = os.getenv("CONNECTOR_NAME", "otx_pulses_connector")
CITY = os.getenv("CITY", "")

if not OTX_API_KEY:
    raise SystemExit("OTX_API_KEY missing in environment. Create .env from .env.sample and set OTX_API_KEY")

# -- logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("otx-etl")

# -- Mongo client
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# -- HTTP session with header
session = requests.Session()
session.headers.update({"X-OTX-API-KEY": OTX_API_KEY, "User-Agent": f"otx-etl/1.0 ({CONNECTOR_NAME})"})

# -- utility: exponential backoff for rate limiting
def safe_get(url: str, params: Optional[Dict[str, Any]] = None, max_retries: int = 5) -> requests.Response:
    """GET with basic retries and handling for 429 Retry-After."""
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, params=params, timeout=30)
        except requests.RequestException as exc:
            logger.warning("Request exception (attempt %d/%d): %s", attempt, max_retries, exc)
            time.sleep(backoff)
            backoff *= 2
            continue

        if resp.status_code == 200:
            return resp
        if resp.status_code == 429:
            # Rate limited
            retry_after = resp.headers.get("Retry-After")
            sleep_for = float(retry_after) if retry_after and retry_after.isnumeric() else backoff
            logger.warning("Rate limited (429). Sleeping for %s seconds.", sleep_for)
            time.sleep(sleep_for)
            backoff *= 2
            continue
        if 500 <= resp.status_code < 600:
            # server errors - retry
            logger.warning("Server error %d. Retrying after %s seconds.", resp.status_code, backoff)
            time.sleep(backoff)
            backoff *= 2
            continue

        # For other client errors, raise to let caller handle (e.g., 401, 403, 404)
        resp.raise_for_status()

    raise RuntimeError(f"Max retries exceeded for GET {url}")

# -- Extract
def fetch_subscribed_pulses(per_page: int = 50, max_pages: int = 100) -> Generator[Dict[str, Any], None, None]:
    """
    Generator to iterate over pulses from /pulses/subscribed using pagination.
    per_page: number of items per page (OTX integrations often limit to 50)
    max_pages: safety limit to avoid runaway loops
    """
    endpoint = f"{BASE_URL}/pulses/subscribed"
    page = 1
    while page <= max_pages:
        params = {"limit": per_page, "page": page}
        logger.info("Fetching page %d from %s", page, endpoint)
        resp = safe_get(endpoint, params=params)
        data = resp.json()
        # OTX response typically has 'results' or 'pulses' depending on endpoint
        items = data.get("results") or data.get("pulses") or data
        if not items:
            logger.info("No items on page %d; stopping.", page)
            break

        # If endpoint returned a dict with metadata, handle list extraction
        if isinstance(items, dict):
            # try to find first list inside dict
            for v in items.values():
                if isinstance(v, list):
                    items = v
                    break

        if not isinstance(items, list):
            logger.info("Unexpected shape for items on page %d: %s", page, type(items))
            break

        for item in items:
            yield item

        # stopping condition: if fewer than per_page results, done
        if len(items) < per_page:
            logger.info("Last page reached (len(items) < per_page).")
            break

        page += 1

# -- Transform
def transform_pulse(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform the raw pulse JSON from OTX into a Mongo-friendly doc.
    Adds metadata fields: ingestion_timestamp, connector_name, source_city (if provided).
    This is intentionally conservative: we keep the raw JSON within 'raw' and expose
    extracted top-level fields for easier queries.
    """
    transformed = {
        "ingestion_timestamp": datetime.utcnow(),
        "connector_name": CONNECTOR_NAME,
        "source": "otx",
        "source_base_url": BASE_URL,
        "source_city": CITY or None,
        "raw": raw,
    }

    # Push a couple of common searchable fields if present
    # (name, id, created, modified, pulse_count, indicator_count)
    pulse_info = raw.get("pulse_info") if isinstance(raw, dict) else None
    if pulse_info:
        transformed["pulse_name"] = pulse_info.get("name")
        transformed["pulse_id"] = pulse_info.get("id")
        # many pulses expose 'modified' or 'created'
        transformed["pulse_created"] = pulse_info.get("created")
        transformed["pulse_modified"] = pulse_info.get("modified")

    # Some API returns id at top-level
    if raw.get("id"):
        transformed["pulse_id"] = transformed.get("pulse_id") or raw.get("id")

    # If indicator_count or counts available
    if raw.get("indicator_count") is not None:
        transformed["indicator_count"] = raw.get("indicator_count")

    return transformed

# -- Load
def upsert_to_mongo(documents: List[Dict[str, Any]], upsert_on: Optional[str] = "pulse_id"):
    """
    Insert documents into MongoDB. For pulses we try to upsert by pulse_id if present,
    otherwise insert as new document.
    """
    if not documents:
        return

    operations = []
    for doc in documents:
        pulse_id = doc.get(upsert_on)
        if pulse_id:
            # replace / upsert by pulse_id
            try:
                collection.replace_one({"pulse_id": pulse_id}, doc, upsert=True)
            except errors.PyMongoError as e:
                logger.error("Mongo replace_one error for pulse_id=%s: %s", pulse_id, e)
        else:
            # fallback: insert as new document
            try:
                collection.insert_one(doc)
            except errors.DuplicateKeyError:
                logger.warning("DuplicateKey on insert; skipping")
            except errors.PyMongoError as e:
                logger.error("Mongo insert_one error: %s", e)

# -- Validation helper
def validate_document(doc: Dict[str, Any]) -> bool:
    """
    Basic validation: check we have ingestion timestamp and raw content.
    Extend this for more rules in assignment (e.g., required fields).
    """
    if "ingestion_timestamp" not in doc:
        return False
    if "raw" not in doc:
        return False
    return True

# -- main run
def main(batch_size: int = 20):
    logger.info("Starting OTX ETL; target collection: %s.%s", DB_NAME, COLLECTION_NAME)

    docs_to_insert = []
    count = 0

    try:
        # ensure DB connection
        client.admin.command("ping")
    except Exception as e:
        logger.exception("Could not connect to MongoDB: %s", e)
        return

    for raw in fetch_subscribed_pulses(per_page=50):
        transformed = transform_pulse(raw)
        if not validate_document(transformed):
            logger.warning("Validation failed for item; skipping")
            continue

        docs_to_insert.append(transformed)
        count += 1

        # Insert in batches
        if len(docs_to_insert) >= batch_size:
            upsert_to_mongo(docs_to_insert)
            logger.info("Inserted/Upserted %d documents so far.", count)
            docs_to_insert.clear()

    # insert any remaining docs
    if docs_to_insert:
        upsert_to_mongo(docs_to_insert)
        logger.info("Inserted/Upserted final %d documents.", len(docs_to_insert))

    logger.info("ETL finished. Total processed: %d", count)

if __name__ == "__main__":
    main()
