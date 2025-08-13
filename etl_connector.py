from dotenv import load_dotenv
import os
import requests
import time
from pymongo import MongoClient, errors, UpdateOne

# Load environment variables from .env
load_dotenv()

# Retry-based API fetch function with timeout
def fetch_with_retries(url, params=None, headers=None, retries=3, backoff_factor=1):
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            print(f"Status Code: {response.status_code}")  # Debug
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:  # Rate limit
                wait_time = backoff_factor * (2 ** attempt)
                print(f"Rate limited. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"Error: Received status code {response.status_code} - {response.text}")
                return None
        except requests.RequestException as e:
            wait_time = backoff_factor * (2 ** attempt)
            print(f"Request failed: {e}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
    print("Failed to fetch data after retries.")
    return None

# Transform API response into list of dictionaries
def transform_pulse(raw):
    if not raw or "results" not in raw:
        raise ValueError("Invalid or incomplete data received from API")

    transformed_list = []
    for pulse in raw.get("results", []):
        transformed = {
            "id": pulse.get("id"),
            "name": pulse.get("name"),
            "description": pulse.get("description"),
            "author": pulse.get("author_name"),
            "created": pulse.get("created"),
            "modified": pulse.get("modified"),
            "tags": pulse.get("tags", []),
            "threats": pulse.get("threats", []),
            "references": pulse.get("references", []),
            "indicators_count": pulse.get("indicator_count", 0),
            "ingested_at": int(time.time())
        }
        transformed_list.append(transformed)
    return transformed_list

# Insert or update documents in MongoDB to avoid duplicates
def load_to_mongo(docs):
    if not docs:
        print("No documents to insert.")
        return

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        operations = []
        for doc in docs:
            # Upsert ensures no duplicates by 'id' field
            operations.append(
                UpdateOne({"id": doc["id"]}, {"$set": doc}, upsert=True)
            )

        if operations:
            result = collection.bulk_write(operations)
            print(f"Inserted: {result.upserted_count}, Modified: {result.modified_count}")
    except errors.ServerSelectionTimeoutError:
        print("Could not connect to MongoDB server.")
    except Exception as e:
        print(f"Insert failed: {e}")
    finally:
        client.close()

# Environment variables
API_KEY = os.getenv("API_KEY")
BASE_URL = os.getenv("BASE_URL")  # e.g., https://otx.alienvault.com/api/v1/pulses/subscribed
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

def main():
    required_vars = [API_KEY, BASE_URL, MONGO_URI, DB_NAME, COLLECTION_NAME]
    if not all(required_vars):
        print("ERROR: One or more required environment variables are missing in .env")
        return

    headers = {"X-OTX-API-KEY": API_KEY}
    page = 1

    while True:
        print(f"Requesting page {page} of subscribed pulses from OTX...")
        params = {"page": page}
        raw = fetch_with_retries(BASE_URL, headers=headers, params=params, retries=4, backoff_factor=2)

        if not raw or "results" not in raw or len(raw["results"]) == 0:
            print("No more pulses to fetch or failed to fetch data.")
            break

        try:
            docs = transform_pulse(raw)
            print(f"Page {page}: fetched {len(docs)} pulses")
            if docs:
                load_to_mongo(docs)
        except Exception as e:
            print(f"Transform failed on page {page}: {e}")
            break

        page += 1

    print("ETL run completed.")

if __name__ == "__main__":
    main()
