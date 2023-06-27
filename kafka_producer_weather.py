import datetime
import json
import random
import uuid
from time import sleep
from kafka import KafkaProducer
import pandas as pd


class WeatherData:
    def __init__(self, weather_id, area, temperature, humidity, timestamp):
        self.weather_id = weather_id
        self.area = area
        self.temperature = temperature
        self.humidity = humidity
        self.timestamp = timestamp

    def to_dict(self):
        return {
            'id': self.weather_id,
            'area': self.area,
            'temperature': self.temperature,
            'humidity': self.humidity,
            'timestamp': self.timestamp
        }


class WeatherProducer:
    def __init__(self, bootstrap_servers, topic, data_frame):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.data_frame = data_frame

    def send_weather_data(self, weather_data):
        message = weather_data.to_dict()
        self.producer.send(self.topic, value=message)
        # print(f"sent -- {message['id']}")

    def start_producing(self, interval):
        weather_data_list = []
        for _, row in self.data_frame.iterrows():
            weather_id = str(uuid.uuid4())
            area = row['area']
            temperature = row['temperature']
            humidity = row['humidity']
            timestamp = datetime.datetime.now().isoformat()

            weather_data = WeatherData(weather_id, area, temperature, humidity, timestamp)
            weather_data_list.append(weather_data)

        for weather_data in weather_data_list:
            self.send_weather_data(weather_data)
            sleep(interval)

        self.producer.flush()
        self.producer.close()


# Example usage
bootstrap_servers = ['localhost:29092']  # Replace with your Kafka broker addresses
topic = 'weather_tp'  # Replace with your topic name

# Assuming you have a DataFrame called 'weather_df' containing the weather data
weather_df = pd.DataFrame({
    'area': ['New York', 'New York', 'New York'],
    'temperature': [25.3, 27.1, 23.8],
    'humidity': [52.4, 45.6, 57.2]
})

weather_producer = WeatherProducer(bootstrap_servers, topic, weather_df)
weather_producer.start_producing(interval=900)
