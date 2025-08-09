from dotenv import load_dotenv
import os
import requests
import time
from pymongo import MongoClient, errors

# Load environment variables from .env
load_dotenv()

# Retry-based API fetch function
def fetch_with_retries(url, params=None, headers=None, retries=3, backoff_factor=1):
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, headers=headers)
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
            print(f"Request failed: {e}")
            wait_time = backoff_factor * (2 ** attempt)
            print(f"Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
    print("Failed to fetch data after retries.")
    return None

# Transform function
def transform_weather(raw):
    """Extracts and formats weather data into a structured dictionary."""
    if not raw or "main" not in raw or "weather" not in raw:
        raise ValueError("Invalid or incomplete data received from API")

    transformed = {
        "city": raw.get("name"),
        "country": raw.get("sys", {}).get("country"),
        "temperature": raw.get("main", {}).get("temp"),
        "feels_like": raw.get("main", {}).get("feels_like"),
        "humidity": raw.get("main", {}).get("humidity"),
        "weather": raw.get("weather", [{}])[0].get("description"),
        "wind_speed": raw.get("wind", {}).get("speed"),
        "timestamp": raw.get("dt"),  # Unix timestamp
        "ingested_at": int(time.time())
    }
    return transformed

# Load function
def load_to_mongo(doc):
    """Inserts the document into MongoDB."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        collection.insert_one(doc)
    except errors.ServerSelectionTimeoutError:
        raise ConnectionError("Could not connect to MongoDB server.")
    finally:
        client.close()

# Environment variables
API_KEY = os.getenv("API_KEY")
BASE_URL = os.getenv("BASE_URL")
CITY = os.getenv("CITY")
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

def main():
    required_vars = [API_KEY, BASE_URL, CITY, MONGO_URI, DB_NAME, COLLECTION_NAME]
    if not all(required_vars):
        print("ERROR: One or more required environment variables are missing in .env")
        return

    params = {
        "q": CITY,
        "appid": API_KEY,
        "units": "metric"
    }

    print(f"Requesting weather for city: {CITY}")
    raw = fetch_with_retries(BASE_URL, params=params, retries=4, backoff_factor=2)

    if not raw:
        print("ERROR: No data returned from API. Exiting.")
        return

    print("Raw data received from API:")
    print(raw)

    try:
        doc = transform_weather(raw)
    except Exception as e:
        print(f"Transform failed: {e}")
        return

    print("Transformed document ready to insert:")
    print(doc)

    try:
        load_to_mongo(doc)
        print("Data loaded to MongoDB successfully.")
    except Exception as e:
        print(f"Load failed: {e}")

if __name__ == "__main__":
    main()
# Updated by K.Keerthana (3122225001060)
