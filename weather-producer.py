import json
import time
import os
import requests
from confluent_kafka import Producer
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Load environment variables from .env file
load_dotenv()

# List of cities
cities = [
    "Nairobi",
    "Johannesburg",
    "Casablanca",
    "Lagos",
    "Mombasa"
]

# OpenWeatherMap API configuration

owm_base_url = "https://api.openweathermap.org/data/2.5/weather"

def fetch_weather_data(city):
    """Fetch weather data from OpenWeatherMap API using city name."""
    url = f"{owm_base_url}?q={city}&appid=6b2b158ff5facbe68dd7b2960b68738a&units=metric"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        data["extracted_city"] = city
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data for {city}: {e}")
        return None

def delivery_report(err, msg):
    """Callback for Kafka message delivery status."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}") 

#Kafka configuration
kafka_config = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVER'),
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.getenv('KAFKA_API_KEY'),
    "sasl.password": os.getenv('KAFKA_API_SECRET'),
    "broker.address.family": "v4",
    "message.send.max.retries": 5,
    "retry.backoff.ms": 500,
}

producer = Producer(kafka_config)
topic = "weather"

def produce_weather_data():
    """Fetch weather data for each city and produce to Kafka."""
    for city in cities:
        data = fetch_weather_data(city)
        if data:
            producer.produce(topic, key=city, value=json.dumps(data), callback=delivery_report)
            producer.poll(0)
        else:
            logger.error(f"Failed to fetch data for {city}")
    producer.flush()

if __name__ == "__main__":
    produce_weather_data()
    logger.info("Data extraction and production complete")
