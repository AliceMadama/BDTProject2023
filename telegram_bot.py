from __future__ import annotations

import datetime
import json
import threading
from kafka import KafkaProducer
import telebot
from database_manager import DatabaseManager
from random import randint
import uuid
from geopy.geocoders import Nominatim


class RouteRequest:
    """
        Represents a route request from source to destination
        """

    def __init__(self, user_id: str, src: dict, dst: dict):
        self.id = str(uuid.uuid4())
        self.created_date = datetime.datetime.now().isoformat()
        self.user_id = user_id
        self.src = src
        self.dst = dst

    def to_dict(self):
        return {
            'id': self.id,
            'created_date': self.created_date,
            'user_id': self.user_id,
            'src': self.src,
            'dst': self.dst
        }


class User:
    """
    Class of a customer
    """

    def __init__(self, id: str, first_name: str, last_name: str, email: str, user_name: str) -> None:
        self.id = id
        self.first_name = first_name
        self.last_name = last_name
        self.user_name = user_name
        self.email = email

    @staticmethod
    def from_repr(data: dict) -> User:
        return User(data['id'], data['first_name'], data['last_name'], data['email'], data['user_name'])

    @staticmethod
    def to_repr(user: User) -> dict:
        return {
            'id': user.id,
            'first_name': user.first_name,
            'last_name': user.last_name,
            'email': user.email,
            'username': user.user_name
        }


class TelegramBot:
    def __init__(self, token):
        self.TOKEN = token
        self.bot = telebot.TeleBot(self.TOKEN)
        self.user_manager = User_checker()
        self.user_data = {}

        # Define user states
        self.STATE_INITIAL = 'initial'
        self.STATE_SOURCE = 'source'
        self.STATE_DESTINATION = 'destination'

        # Initialize user state
        self.user_states = {}
        self.user_address_src = {}
        self.user_address_dst = {}

        # kafka
        self.KAFKA_TOPIC = 'route_request_tpp'
        self.bootstrap_servers = ['localhost:29092']

    def start_bot(self):
        @self.bot.message_handler(commands=['start'])
        def send_welcome(message):
            self.bot.reply_to(message,
                              "Welcome to the GeoJSON graph bot! Send any message and I'll generate a Folium graph for you.")

        @self.bot.message_handler(func=lambda message: True)
        def handle_message(message):
            user_id = message.from_user.id
            state = self.get_user_state(user_id)

            if state == self.STATE_INITIAL:
                # Ask for the source coordinate
                self.bot.reply_to(message, "Please enter the source coordinate:")
                self.set_user_state(user_id, self.STATE_SOURCE)
            elif state == self.STATE_SOURCE:
                source_address = message.text
                coordinate = self.get_coordinate(source_address)
                if not self.validate_coordinate(message, coordinate,
                                                "The coordinate is not within the New York area. Please try again."):
                    return  # Exit the function without creating a new instance

                self.user_address_src[user_id] = coordinate
                self.bot.reply_to(message, "Source coordinate received. Now, enter the destination coordinate:")
                self.set_user_state(user_id, self.STATE_DESTINATION)

            elif state == self.STATE_DESTINATION:
                # Retrieve the destination coordinate
                destination_address = message.text
                coordinate = self.get_coordinate(destination_address)
                if not self.validate_coordinate(message, coordinate,
                                                "The coordinate is not within the New York area. Please try again."):
                    return  # Exit the function without creating a new instance

                self.user_address_dst[user_id] = coordinate
                self.bot.reply_to(message, f"Destination coordinate received. Thank you!")

                # Process data
                print(self.user_address_src[user_id])
                print(self.user_address_dst[user_id])

                route = RouteRequest(user_id, self.user_address_src[user_id], self.user_address_dst[user_id])
                print(route.to_dict())

                # Send route request to Kafka using threading
                threading.Thread(target=self.send_route_request_to_kafka, args=(route,)).start()

                # Reset user state
                self.set_user_state(user_id, self.STATE_INITIAL)

        self.bot.polling()

    def is_coordinate_in_new_york(self, coordinate):
        # Define the New York area boundaries
        min_latitude = 40.5
        max_latitude = 4141.4547557
        min_longitude = -74.5
        max_longitude = -73.5
        latitude = coordinate['latitude']
        longitude = coordinate['longitude']
        if min_latitude <= latitude <= max_latitude and min_longitude <= longitude <= max_longitude:
            return True

        return False

    def validate_coordinate(self, message, coordinate, error_message):
        if coordinate is None:
            self.bot.reply_to(message, "Invalid coordinate. Please try again.")
            return False

        if not self.is_coordinate_in_new_york(coordinate):
            self.bot.reply_to(message, error_message)
            return False

        return True

    def send_route_request_to_kafka(self, route):
        producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers)
        route_json = json.dumps(route.to_dict())
        producer.send(self.KAFKA_TOPIC, value=route_json.encode('utf-8'))
        producer.flush()
        producer.close()

    def get_user_state(self, user_id):
        return self.user_states.get(user_id, self.STATE_INITIAL)

    def set_user_state(self, user_id, state):
        self.user_states[user_id] = state

    def get_coordinate(self, address):
        print(address)
        geolocator = Nominatim(user_agent="my-application1234")
        src = geolocator.geocode(address)
        if src is None:
            return None
        points = {'latitude': src.latitude, 'longitude': src.longitude}
        print(points)

        return points


class User_checker:
    def __init__(self, database_name="traffic_management", collection_name="customers") -> None:
        """
        Creates some default customers if not already present into database
        """
        # Connection to database
        self.__db_manager = DatabaseManager()
        self.__db_manager.connect_to_database(database_name, collection_name)
        self.__db_manager.select_collection(collection_name)

    def is_user_exist(self, user_id):
        res = self.__db_manager.find_one({'id': user_id})
        return res

    def generate(self, user_id, first_name, last_name, user_name, email, exist) -> User:
        """
       Create user object
        """
        user = User(
            user_id if exist else str(uuid.uuid4()),
            first_name,
            last_name,
            email,
            user_name
        )
        return user

    def save_to_db(self, user):
        self.__db_manager.insert_document([User.to_repr(user)])


if __name__ == "__main__":
    TOKEN = 'x'  # Replace with your Telegram bot token
    bot = TelegramBot(TOKEN)
    bot.start_bot()
