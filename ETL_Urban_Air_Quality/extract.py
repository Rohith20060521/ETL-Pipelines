import json
import os
import requests
from datetime import datetime
from pathlib import Path
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants and City Coordinates
BASE_DIR = Path(__file__).resolve().parents[0]
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

CITIES = {
    "Delhi": {"lat": 28.7041, "lon": 77.1025},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777},
    "Bengaluru": {"lat": 12.9716, "lon": 77.5946},
    "Hyderabad": {"lat": 17.3850, "lon": 78.4867},
    "Kolkata": {"lat": 22.5726, "lon": 88.3639}
}

API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
MAX_RETRIES = 3

def fetch_air_quality_data(city: str, lat: float, lon: float, retries: int = MAX_RETRIES):
    """
    Fetch air quality data for the given city from Open-Meteo API with retry logic.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide,uv_index"
    }
    
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"⏳ Requesting air quality data for {city} (Attempt {attempt}/{retries}) ...")
            response = requests.get(API_URL, params=params, timeout=30)
            response.raise_for_status()  # Will raise an HTTPError for bad responses
            data = response.json()
            
            if not data.get("hourly"):
                logging.warning(f"⚠️ No hourly data found for {city}. Empty response.")
                return None
            
            return data
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Error fetching data for {city}: {e}")
            if attempt < retries:
                logging.info("🔄 Retrying...")
                time.sleep(2)  # Wait before retrying
            else:
                logging.error(f"❌ Failed to fetch data for {city} after {retries} attempts.")
                return None

def save_air_quality_data(city: str, data: dict):
    """
    Save air quality data to a JSON file with a timestamp.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = RAW_DIR / f"{city.lower()}_raw_{timestamp}.json"
    
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    
    logging.info(f"✅ Saved air quality data for {city} to {filename}")
    return str(filename)

def extract_air_quality_data_for_cities(cities: dict):
    """
    Fetch and save air quality data for a list of cities.
    """
    saved_files = []
    
    for city, coords in cities.items():
        lat, lon = coords["lat"], coords["lon"]
        data = fetch_air_quality_data(city, lat, lon)
        if data:
            file_path = save_air_quality_data(city, data)
            saved_files.append(file_path)
    
    return saved_files

if __name__ == "__main__":
    saved_files = extract_air_quality_data_for_cities(CITIES)
    if saved_files:
        logging.info(f"✅ All data saved successfully. Files: {saved_files}")
    else:
        logging.warning("⚠️ No data saved. Please check the logs for errors.")