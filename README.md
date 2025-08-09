Weather Data ETL Connector

Project Overview

This project implements a Python-based ETL (Extract, Transform, Load) connector that retrieves current weather data for a specified city from the OpenWeatherMap API. The connector extracts JSON data from the API, transforms it into a structured format compatible with MongoDB, and loads it into a MongoDB collection for storage and further analysis.

API Provider Details

API Provider: OpenWeatherMap
Base URL: `https://api.openweathermap.org/data/2.5/weather`
Authentication Method: API key authentication, passed as a query parameter (`appid`)
Endpoint Used: Current weather data endpoint for a single city

Setup Instructions

1. Clone the repository and navigate to the project directory.
2. Create and activate a Python virtual environment:

    On Unix/macOS:

     ```bash
     python3 -m venv venv
     source venv/bin/activate
     ```
    On Windows:

     ```bash
     python -m venv venv
     venv\Scripts\activate
     ```
3. Install required dependencies using the provided requirements file:

   ```bash
   pip install -r requirements.txt
   ```
4. Create a `.env` file in the project root directory. The file should contain the following environment variables with appropriate values (do not include quotation marks):

   ```
   API_KEY=your_openweathermap_api_key
   BASE_URL=https://api.openweathermap.org/data/2.5/weather
   CITY=Chennai
   MONGO_URI=mongodb://localhost:27017/
   DB_NAME=weather_data
   COLLECTION_NAME=weather_reports
   ```

   It is important to note that the `.env` file contains sensitive information and should not be committed to version control.

## Running the ETL Script

To execute the ETL pipeline, run the following command in your terminal or command prompt from the project directory:

```bash
python etl_connector.py
```

The script will connect to the OpenWeatherMap API, retrieve the weather data for the specified city, transform the data into a structured format, and insert it into the configured MongoDB collection.

Data Structure in MongoDB

Each document inserted into the MongoDB collection contains the following fields:

* `city`: Name of the city for which weather data is retrieved
* `country`: ISO country code of the city
* `temperature`: Current temperature in degrees Celsius
* `feels_like`: Perceived temperature in degrees Celsius
* `humidity`: Humidity percentage
* `weather`: Textual description of the weather conditions
* `wind_speed`: Wind speed in meters per second
* `timestamp`: Unix timestamp indicating the time of the weather data
* `ingested_at`: Unix timestamp recording when the data was ingested into MongoDB

Error Handling and Validation

The script implements the following error handling mechanisms:

* Validation of required environment variables before execution begins.
* Handling of invalid API responses, including non-success HTTP status codes and empty or malformed JSON responses.
* Retry logic with exponential backoff in the event of HTTP 429 (Too Many Requests) rate limiting errors.
* Exception handling for network errors during API requests.
* Exception handling for MongoDB connection and insertion failures, including timeout detection.
* Informative console output for success and error states to facilitate debugging.

Assumptions and Limitations

* The API key provided in the `.env` file is valid and has sufficient permissions to access the OpenWeatherMap API.
* The MongoDB server specified by `MONGO_URI` is accessible and running at the time of script execution.
* The script is designed for single-city data retrieval and does not currently support batch or multi-city queries.
* Scheduling, automation, and user interface components are not included and may be implemented separately as needed.
* The script is intended primarily for educational and demonstration purposes within the scope of this assignment.

Author Information

* Name: K.Keerthana
* Roll Number: 3122225001060

---

