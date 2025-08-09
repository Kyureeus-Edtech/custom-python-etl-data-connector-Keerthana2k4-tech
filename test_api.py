import requests

API_KEY = "0b2a1ec6ee073ff15fa6783b90513769"
CITY = "Chennai"
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

params = {
    "q": CITY,
    "appid": API_KEY,
    "units": "metric"
}

response = requests.get(BASE_URL, params=params)
print("Status code:", response.status_code)
print("Response:", response.text)
