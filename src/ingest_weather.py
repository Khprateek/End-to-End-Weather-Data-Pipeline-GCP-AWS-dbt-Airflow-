import requests
import json
from datetime import datetime
import os

from config.config import API_KEY, CITY, BASE_URL


def fetch_weather():
    params = {
        "q": CITY,
        "appid": API_KEY,
        "units": "metric"
    }

    response = requests.get(BASE_URL, params=params)

    if response.status_code != 200:
        raise Exception(f"API Error: {response.status_code}, {response.text}")

    return response.json()


def transform_data(raw_data):
    transformed = {
        "city": raw_data["name"],
        "temperature": raw_data["main"]["temp"],
        "humidity": raw_data["main"]["humidity"],
        "weather": raw_data["weather"][0]["description"],
        "wind_speed": raw_data["wind"]["speed"],
        "timestamp": datetime.utcnow().isoformat()
    }

    return transformed


def save_data(data):
    today = datetime.utcnow().strftime("%Y-%m-%d")
    folder_path = f"data/raw/{today}"

    os.makedirs(folder_path, exist_ok=True)

    file_path = f"{folder_path}/weather.json"

    with open(file_path, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Data saved to {file_path}")


if __name__ == "__main__":
    raw = fetch_weather()
    clean = transform_data(raw)
    save_data(clean)