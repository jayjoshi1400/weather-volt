import json
import time
import os
import logging
from datetime import datetime
import requests
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('openweathermap-producer')

# Kafka configuration
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'openweathermap-weather')

# OpenWeatherMap API configuration
API_KEY = os.environ.get('OPENWEATHERMAP_API_KEY')
if not API_KEY:
    raise ValueError("OpenWeatherMap API key not provided. Set OPENWEATHERMAP_API_KEY environment variable.")

# North Carolina locations
LOCATIONS = [
    {"city": "Raleigh", "lat": 35.7796, "lon": -78.6382},
    {"city": "Charlotte", "lat": 35.2271, "lon": -80.8431},
    {"city": "Greensboro", "lat": 36.0726, "lon": -79.7920},
    {"city": "Asheville", "lat": 35.5951, "lon": -82.5515},
    {"city": "Wilmington", "lat": 34.2257, "lon": -77.9447}
]
API_URL = "http://api.openweathermap.org/data/2.5/weather"

def create_producer():
    """Create and return a Kafka producer"""
    retry_count = 0
    max_retries = 10
    
    while retry_count < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if isinstance(k, str) else str(k).encode('utf-8'),
                retries=3
            )
            logger.info(f"Successfully connected to Kafka broker at {KAFKA_BROKER}")
            return producer
        except Exception as e:
            retry_count += 1
            logger.warning(f"Failed to connect to Kafka broker (attempt {retry_count}/{max_retries}): {str(e)}")
            time.sleep(5)
    
    raise ConnectionError(f"Failed to connect to Kafka broker after {max_retries} attempts")

def fetch_weather_data():
    """Fetch weather data for all locations."""
    weather_data = []
    fetch_time = datetime.utcnow().isoformat()
    
    for location in LOCATIONS:
        params = {
            'lat': location['lat'],
            'lon': location['lon'],
            'appid': API_KEY,
            'units': 'metric'  # Use metric units (C, m/s)
        }
        try:
            response = requests.get(API_URL, params=params)
            if response.status_code == 200:
                data = response.json()
                # Add metadata
                data['city'] = location['city']
                data['fetch_timestamp'] = fetch_time
                data['source'] = 'openweathermap'
                weather_data.append(data)
                logger.info(f"Successfully fetched data for {location['city']}")
            else:
                logger.error(f"Error fetching data for {location['city']}: {response.status_code} {response.text}")
        except Exception as e:
            logger.error(f"Exception fetching data for {location['city']}: {str(e)}")
    
    return weather_data

def stream_weather_data():
    """Stream weather data to Kafka."""
    producer = create_producer()
    
    while True:
        try:
            weather_data = fetch_weather_data()
            for data in weather_data:
                # Use city name as the key for partitioning
                key = data['city']
                producer.send(KAFKA_TOPIC, key=key, value=data)
                logger.info(f"Sent data to Kafka for city: {data['city']}")
            
            # Ensure all messages are sent
            producer.flush()
            logger.info(f"Fetched and sent data for {len(weather_data)} locations")
            
            # Wait 10 minutes before fetching again 
            logger.info("Waiting 10 minutes before next data fetch...")
            time.sleep(600)
        except Exception as e:
            logger.error(f"Error in stream_weather_data: {str(e)}")
            time.sleep(60)  # Wait a minute before retrying

if __name__ == "__main__":
    logger.info("Starting OpenWeatherMap Kafka producer...")
    stream_weather_data()
