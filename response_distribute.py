import datetime
import io
import json
import uuid

import folium
import pandas as pd
import telebot
from colorama import Fore, Style
from kafka import KafkaConsumer, KafkaProducer

from database_manager import DatabaseManager
from redis_manager import RedisManager


class ProcessedResult:
    def __init__(self, id, request_id, route_details, travel_time, distance, congestion_level, weather_conditions,
                 timestamp):
        self.id = id
        self.request_id = request_id
        self.route_details = route_details
        self.travel_time = travel_time
        self.distance = distance
        self.congestion_level = congestion_level
        self.weather_conditions = weather_conditions
        self.timestamp = timestamp

    def to_dict(self):
        return {
            'id': self.id,
            'request_id': self.request_id,
            'route_details': self.route_details,
            'travel_time': self.travel_time,
            'distance': self.distance,
            'congestion_level': self.congestion_level,
            'weather_conditions': self.weather_conditions,
            'timestamp': self.timestamp
        }


class TelegramBot:
    def __init__(self, token):
        self.TOKEN = token
        self.bot = telebot.TeleBot(self.TOKEN)


class Response:
    bootstrap_servers = ['localhost:29092']
    TOKEN = '6231783852:AAGyqchKx1IbuulyTEPARPN4uCtDNpoehJk'  # Replace with your Telegram bot token

    bot = telebot.TeleBot(TOKEN)

    def __init__(self):
        self.mongo_db_manager = DatabaseManager()
        self.redis_manager = RedisManager()
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers)
        self.consumer = KafkaConsumer(
            'route_response_tp',
            bootstrap_servers=self.bootstrap_servers,
            group_id='result_consumer_group',
            auto_offset_reset='latest'
        )

    def process_record(self, response):
        # Parse the JSON response
        request_id = response["id"]

        fts = json.loads(response["processed_dst"])

        congestion_level = fts['congestion_level']
        weather_conditions = fts['weather_condition']
        timestamp = datetime.datetime.now().isoformat()

        # # Create a list to store the processed records
        processed_records = []

        if fts["features"] is None:
            print(None)
            return []
        # Iterate over each route
        for route in fts["features"]:
            # Create an instance of ProcessedResult with the extracted values for each route
            processed_record = ProcessedResult(
                id=str(uuid.uuid4()),
                request_id=request_id,
                route_details=route,
                travel_time=route["properties"]["summary"]["duration"],
                distance=route["properties"]["summary"]["distance"],
                congestion_level=congestion_level,
                weather_conditions=weather_conditions,
                timestamp=timestamp
            )

            # Add the processed record to the list
            processed_records.append(processed_record.to_dict())

        # Return the list of processed records
        return processed_records
        # return dict("")

    def consume_responses(self):

        # Consume messages from the Kafka topic
        for message in self.consumer:
            if message is None:
                continue

            # Decode and process the message

            record = json.loads(message.value.decode('utf-8'))

            # Perform your processing logic here
            processed_records = self.process_record(record)

            # Save the processed record to MongoDB
            self.mongo_db_manager.connect_to_database("traffic_management", "route_response_tp")
            self.mongo_db_manager.insert_document(processed_records)

            cache_key = f"route_request_tpp:{record['id']}"
            route_request = self.redis_manager.get_data_from_cache(cache_key)
            # print(f'route request from cache: {route_request}')
            if route_request is None:
                continue
            route_request = json.loads(route_request)
            user_id = route_request["user_id"]

            print(
                Fore.CYAN + f"{datetime.datetime.now().isoformat()}--"
                            f" pushing to db {record['id']} of {user_id} " + Style.RESET_ALL)

            if len(str(route_request["user_id"])) < 20:  # generated user if > 20 else simulated user
                print(
                    Fore.YELLOW + f"Telegram request" + Style.RESET_ALL)
                self.send_response_to_user(route_request["user_id"], processed_records)
            else:
                print(
                    Fore.YELLOW + f"Simulation request" + Style.RESET_ALL)
                continue
        self.consumer.commit()
        # Close the Kafka consumer
        self.consumer.close()

    def send_response_to_user(self, user_id, processed_result):

        for result in processed_result:
            # Extract relevant information from the processed result and append it to the response message
            # print(result)
            map_graph = self.create_map(result)
            image1 = map_graph._to_png()
            photo_stream = io.BytesIO(image1)
            photo_stream.seek(0)

            travel_time = result['travel_time'] // 60
            distance = result['distance'] // 1000
            response_message = f"Travel Time: {travel_time} Distance: {distance}\n"
            # Send the response message to the bot with the given user ID

            try:
                # Send the response message to the bot with the retrieved user ID
                self.bot.send_photo(user_id, photo_stream)
                self.bot.send_message(user_id, response_message)
            except telebot.apihelper.ApiTelegramException as e:
                error_message = str(e)
                if "chat_id is empty" in error_message:
                    # Handle the case when the chat_id is empty
                    print("Error: chat_id is empty")
                    # Perform necessary actions or error handling here

        return None

    def create_map(self, gj):

        mls = gj['route_details']['geometry']['coordinates']
        points = [(i[1], i[0]) for i in mls]

        # Create the map
        m = folium.Map()

        # Add marker for the start and ending points
        for point in [points[0], points[-1]]:
            folium.Marker(point).add_to(m)

        # Add the line
        folium.PolyLine(points, weight=5, opacity=1).add_to(m)

        # Create optimal zoom
        data_f = pd.DataFrame(mls).rename(columns={0: 'Lon', 1: 'Lat'})[['Lat', 'Lon']]
        sw = data_f[['Lat', 'Lon']].min().values.tolist()
        ne = data_f[['Lat', 'Lon']].max().values.tolist()
        m.fit_bounds([sw, ne])
        return m


def main():
    after = Response()
    after.consume_responses()


if __name__ == "__main__":
    main()
