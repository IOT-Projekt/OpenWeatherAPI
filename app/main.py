import time
from typing import Dict
import requests
import os
from kafka_handler import KafkaConfig, setup_kafka_producer, send_kafka_message

API_KEY = os.getenv("OPEN_WEATHER_API_KEY")
KAKFA_TOPIC = os.getenv("KAFKA_TOPIC", "openweather_data")

def get_openweather_data():
    # Create the API call URL
    url = f"https://api.openweathermap.org/data/3.0/onecall?lat=49,150002&lon=9,216600&exclude=hourly,daily&appid={API_KEY}"
    
    # Make the API call and return the content, if successful
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

def extract_weather_data(response_json) -> Dict[str, float]:
    # Extract the current weather data from the response
    current_weather = response_json.get("current")
    
    # Return the extracted data as a dictionary
    return {
        "temperature": current_weather.get("temp") - 273.15, # Convert to Celsius from Kelvin
        "feels_like": current_weather.get("feels_like") - 273.15, # Convert to Celsius from Kelvin
        "humidity": current_weather.get("humidity"),
    }

if __name__ == "__main__":
    
    while True:
        # Fetch OpenWeather data
        response_json = get_openweather_data()
        if not response_json:
            print("Failed to fetch OpenWeather data")
        
        # Extract weather data
        weather_dict = extract_weather_data(response_json)
        
        # Get kafka producer
        kafka_config = KafkaConfig()
        openweather_producer = setup_kafka_producer(kafka_config)
        
        # Send the weather data to Kafka
        send_kafka_message(openweather_producer, KAKFA_TOPIC, weather_dict)
        
        time.sleep(10)