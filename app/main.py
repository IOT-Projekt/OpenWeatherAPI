import time
from typing import Dict
import requests
import os
import paho.mqtt.client as mqtt
import logging
import signal
import json

# setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Environment variables
API_KEY = os.getenv("API_KEY")
BROKER_IP = os.getenv("BROKER_IP", "localhost")
CLIENT_ID = os.getenv("CLIENT_ID", "openweather-mqtt")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
TOPIC = os.getenv("TOPIC", "iot/devices/open_weather")
BROKER_PORT = int(os.getenv("BROKER_PORT", 1883))
LATITUDE = os.getenv("LATITUDE", "49.150002")
LONGITUDE = os.getenv("LONGITUDE", "9.216600")

def get_openweather_data():
    # Create the API call URL, see as reference: https://openweathermap.org/api/one-call-api
    url = f"https://api.openweathermap.org/data/3.0/onecall?lat={LATITUDE}&lon={LONGITUDE}&exclude=hourly,daily&appid={API_KEY}"

    # Make the API call and return the content, if successful
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# Callback for connection logging
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to MQTT broker.")
        client.connected_flag = True
    else:
        logging.error(f"Connection failed with return code {rc}")
        client.connected_flag = False

# Callback for publish logging
def on_publish(client, userdata, message_id):
    logging.info(f"Message {message_id} published successfully.")


def shutdown(client):
    """Gracefully stops the MQTT client and disconnects."""
    global RUNNING
    RUNNING = False
    logging.info("Shutting down...")
    if client:
        client.loop_stop()
        client.disconnect()
        logging.info("Disconnected from MQTT broker.")


def handle_signals(signal_num, frame):
    """Handles termination signals for Docker."""
    logging.info(f"Received termination signal: {signal_num}")
    shutdown(client=None)


def configure_mqtt_client(client_id):
    """Configures the MQTT client with callbacks and credentials."""
    logging.info("Configuring MQTT client...")
    client = mqtt.Client(client_id)
    client.connected_flag = False

    # Set username and password for authentication
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    # Bind callbacks
    client.on_connect = on_connect
    client.on_publish = on_publish

    # Register signal handlers for Docker
    signal.signal(signal.SIGTERM, handle_signals)
    signal.signal(signal.SIGINT, handle_signals)

    # Connect to the broker, if not successful try again after 1 second
    try:
        logging.info("Connecting to broker...")
        client.connect(BROKER_IP, BROKER_PORT)
        client.loop_start()
        while not client.connected_flag:
            logging.info("Waiting for connection...")
            time.sleep(1)
    except Exception as e:
        logging.error(f"Failed to connect to MQTT broker: {e}")
        shutdown(client)
        return

    return client


def extract_weather_data(response_json) -> Dict[str, float]:
    # Extract the current weather data from the response
    current_weather = response_json.get("current")

    # Return the extracted data as a dictionary
    return {
        "temperature": current_weather.get("temp")
        - 273.15,  # Convert to Celsius from Kelvin
        "feels_like": current_weather.get("feels_like")
        - 273.15,  # Convert to Celsius from Kelvin
        "humidity": current_weather.get("humidity"),
    }


if __name__ == "__main__":
    # Configure the MQTT client
    client = configure_mqtt_client(CLIENT_ID)
    
    while True:
        # Fetch OpenWeather data
        response_json = get_openweather_data()
        if not response_json:
            logging.error("Failed to fetch OpenWeather data")

        # Extract weather data
        weather_dict = extract_weather_data(response_json)

        # Send the weather data to MQTT broker
        payload = json.dumps(
            {
                "source": "mqtt",
                "device_id": CLIENT_ID,
                "humidity": weather_dict["humidity"],
                "temperature": weather_dict["temperature"],
                "feels_like": weather_dict["feels_like"],
            }
        )
        client.publish(TOPIC, payload)

        # Wait for 2 minutes before fetching new
        time.sleep(120)
