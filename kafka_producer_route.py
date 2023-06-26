import datetime
import json
import random
from random import randrange
from uuid import uuid4
from time import sleep, time
from threading import Thread
from enum import IntEnum
from geopy.distance import distance
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

from user_generator import CustomersGenerator
from open_route_manager import DirectionsAPI


class KafkaProducerWrapper(object):
    __servers = []
    __admin = None
    producer = None
    topic = None

    def __init__(self, servers: list, topic: str) -> None:
        """
        - servers: connections to Kafka brokers
        - topic: topic to create
        """

        class topic_settings(IntEnum):
            PARTITIONS = 2
            REPLICATION_FACTOR = 1

        self.__servers = servers
        self.topic = topic
        self.__admin = KafkaAdminClient(bootstrap_servers=servers)
        self.create_topic(topic, int(topic_settings.PARTITIONS), int(topic_settings.REPLICATION_FACTOR))
        self.create_producer()

    def create_topic(self, topic_name: str, topic_partitions=1, replication_factor=1):
        topic_list = [NewTopic(name=topic_name, num_partitions=topic_partitions, replication_factor=replication_factor)]
        try:
            self.__admin.create_topics(new_topics=topic_list, validate_only=False)
        except TopicAlreadyExistsError as ex:
            # Topic already exists, do nothing
            pass

    def create_producer(self) -> None:
        """
        Creates an instance of the KafkaProducer
        """

        # Producer with JSON encoding
        self.producer = KafkaProducer(bootstrap_servers=self.__servers,
                                      value_serializer=lambda m: json.dumps(m).encode('ascii'))


class RouteRequest:
    """
        Represents a route request from source to destination
        """

    def __init__(self, user_id: str, src: dict, dst: dict):
        self.id = str(uuid4())
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


class CustomerProducer(KafkaProducerWrapper):
    """
    This is a Kafka producer that simulates customer behavior.
    It is capable of creating transactions, e.g., buying items from a retail/shop.
    """

    def __init__(self, servers: list, topic: str, customer: list) -> None:
        super().__init__(servers, topic)
        self.__producer = self.producer
        self.__topic = self.topic

        """
        - servers: connections to Kafka brokers
        - topic: 
        """
        self.__customer_list = customer

    def create_route_request(self, customer=None) -> RouteRequest:
        """
        Generates a request from source to destination
        """
        if not customer:
            customer = self.__customer_list[randrange(0, len(self.__customer_list))]

        def __generate_location_point():
            # Define the latitude and longitude range for the specific area
            # For example, let's consider an area within New York City
            min_latitude = 40.70
            max_latitude = 40.80
            min_longitude = -74.00
            max_longitude = -73.90

            # # Generate random latitude and longitude within the specified range
            # latitude = random.uniform(min_latitude, max_latitude)
            # longitude = random.uniform(min_longitude, max_longitude)

            # Generate random latitude and longitude within the specified range
            latitude1 = random.uniform(min_latitude, max_latitude)
            longitude1 = random.uniform(min_longitude, max_longitude)

            latitude2 = random.uniform(min_latitude, max_latitude)
            longitude2 = random.uniform(min_longitude, max_longitude)

            pt1 = {'longitude': longitude1, 'latitude': latitude1}
            pt2 = {'longitude': longitude2, 'latitude': latitude2}


            # while True:
            #     # Generate random latitude and longitude within the specified range
            #     latitude1 = random.uniform(min_latitude, max_latitude)
            #     longitude1 = random.uniform(min_longitude, max_longitude)
            #
            #     latitude2 = random.uniform(min_latitude, max_latitude)
            #     longitude2 = random.uniform(min_longitude, max_longitude)
            #
            #     pt1 = {'longitude': longitude1, 'latitude': latitude1}
            #     pt2 = {'longitude': longitude2, 'latitude': latitude2}
            #
            #     if self.is_routable(pt1, pt2):
            #         break

            return pt1, pt2

        src, dst = __generate_location_point()
        print(customer)
        return RouteRequest(customer['id'], src, dst)

    def is_routable(self, pt1, pt2):
        # Prepare the API request
        route_manger = DirectionsAPI()
        route_request = {
            'src': [pt1['longitude'], pt1['latitude']],
            'dst': [pt2['longitude'], pt2['latitude']],
        }

        resp = route_manger.get_directions(route_request)
        print(resp)
        # Check if the point is routable
        if 'routes' in resp:
            print(True)
            return True
        else:
            return False

    def send_route_request(self, route_request: RouteRequest, thread_id: int) -> None:
        """
        Sends data to the brokers. Data is a transaction.
        """

        def __on_send_success(record_metadata):
            print(f"Thread [{thread_id}]: [topic: {record_metadata.topic} "
                  f"partition: {record_metadata.partition} offset: {record_metadata.offset}]")

        def __on_send_error(excp):
            print('Error occurred while sending data:', exc_info=excp)

        # Produce asynchronously

        p = self.__producer.send(self.__topic, value=route_request.to_dict()).add_callback(
            __on_send_success).add_errback(
            __on_send_error)

        # Block until all async messages are sent
        self.__producer.flush()

    def stop_stream(self):
        # Stops the streaming of route requests
        pass

    def __activate_route_request_stream(self, thread_id: int, sleep_time: int, simulation_time: int):
        """
        A stream of route requests is sent to the configured retail by the customer.
        """
        start_time = time()

        while True:
            req_rt = self.create_route_request()

            self.send_route_request(req_rt, thread_id)
            sleep(sleep_time)

            # Check end of simulation
            elapsed_time = time() - start_time
            if elapsed_time >= simulation_time:
                break

    def create_producers_threads(self, quantity: int = 10, simulation_time: int = 10) -> list:
        """
        Produces shares the same KafkaProducer instance. Different threads can be spawned to send messages.
        - quantity: how many threads
        - simulation_time: total simulation time in seconds
        """
        sleep_time = 1  # Sleep time in seconds

        assert quantity >= 1

        # Create and start "quantity" threads
        threads = []
        for n in range(quantity):
            t = Thread(target=self.__activate_route_request_stream, args=(n, sleep_time, simulation_time))
            t.daemon = False
            threads.append(t)
            t.start()
        return threads


bootstrap_servers = ['localhost:29092']
topic = 'route_request_tpp'
customers_gen = CustomersGenerator(default_customers=10)
customers = customers_gen.get_customers()

producer = CustomerProducer(bootstrap_servers, topic, customers)

NUM_TRIALS = 1  # Simulation cycles
SIMULATION_TRIAL_TOTAL_TIME = 3000  # Seconds
CUSTOMERS_THREADS = 2

producers_threads = producer.create_producers_threads(CUSTOMERS_THREADS, SIMULATION_TRIAL_TOTAL_TIME)
