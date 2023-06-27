import argparse
import datetime
from json import dumps as json_dump
from json import loads as json_loads

from colorama import Fore, Style
from colorama import init as colorama_init
from kafka import KafkaConsumer
from redis import Redis

from database_manager import DatabaseManager
from redis_manager import RedisManager
import threading
import time


def handle_failed_message(topic, body) -> None:
    """
    Handle failed messages by moving them to a dead-letter queue or taking appropriate action.
    """
    print(Fore.RED + f"Failed to process message from topic '{topic}'. Message: {body}" + Style.RESET_ALL)
    # Implement your logic for handling failed messages here
    # You can move the message to a dead-letter queue for further analysis or manual intervention


class Consumer:
    def __init__(self) -> None:
        self.db_manager = RedisManager().get_instance()
        self.mongo_db_manager = DatabaseManager()
        self.mongo_db_manager.connect_to_database("traffic_management")
        self.kafka_consumers = []
        self.shutdown_event = threading.Event()

    def create_consumer(self, servers: list, topics, topic_groups) -> None:
        """
        Creates consumers for multiple topics.
        """

        # Enable colored print
        colorama_init()
        # Get a Redis db instance
        assert self.db_manager.ping()

        # Create a KafkaConsumer instance for traffic flow and weather topics

        for topic in topics:

            consumer = KafkaConsumer(
                topic,
                group_id=topic_groups.get(topic, "default_consumer_group"),
                bootstrap_servers=servers,
                value_deserializer=lambda m: json_loads(m.decode('ascii'))
            )
            self.kafka_consumers.append(consumer)
            threading.Thread(target=self.consume_periodic_topic, args=(consumer,)).start()

    def consume_periodic_topic(self, consumer: KafkaConsumer) -> None:
        """
        Consume messages from a periodic topic and process them accordingly.
        """
        try:
            for message in consumer:
                topic = message.topic
                body = message.value

                # print(message.value)


                self.process_data(topic, body)
                if self.shutdown_event.is_set():
                    break
        except Exception as e:
            print(Fore.RED + f"Exception in consuming topic '{consumer.subscription()[-1]}': {e}" + Style.RESET_ALL)

    def process_data(self, topic, body) -> None:
        """
        Process traffic flow message and perform necessary actions.
        """
        try:

            # Save data in Redis cache
            if topic == 'route_request_tpp':
                # Process route request streaming data
                cache_key = f"{topic}:{body['id']}"
            else:
                # Process periodic data updates
                cache_key = topic
            cache_data = json_dump(body)

            print(
                Fore.CYAN + f"{datetime.datetime.now().isoformat()}--" 
                            f" pushing {topic} -- {body['id']} " + Style.RESET_ALL)

            # Use a pipeline for atomic operations
            pipeline = self.db_manager.pipeline()
            pipeline.multi()

            # Set the data in the cache with an expiration time of 20 minutes
            retry_count = 0
            while retry_count < 3:
                try:
                    pipeline.set(cache_key, cache_data)
                    pipeline.expire(cache_key, 1200)
                    pipeline.execute()
                    break
                except Exception as e:
                    print(
                        Fore.YELLOW + f"Exception occurred while setting cache data: {e}. Retrying..." + Style.RESET_ALL)
                    retry_count += 1
                    time.sleep(1)  # Add a delay between retries

            if retry_count >= 3:
                handle_failed_message(topic, body)

            retry_count = 0
            while retry_count < 3:
                try:
                    self.mongo_db_manager.insert_document_to_collection(topic, [body])
                    break
                except Exception as e:
                    print(
                        Fore.YELLOW + f"Exception occurred while inserting into MongoDB: {e}. Retrying..." + Style.RESET_ALL)
                    retry_count += 1
                    time.sleep(1)  # Add a delay between retries

            if retry_count >= 3:
                handle_failed_message(topic, body)
        except Exception as e:
            print(Fore.RED + f"Exception occurred while processing data: {e}" + Style.RESET_ALL)

    def shutdown(self):
        """
        Shutdown the consumer gracefully.
        """
        self.shutdown_event.set()
        for consumer in self.kafka_consumers:
            consumer.close()
        self.mongo_db_manager.close_connection()


if __name__ == "__main__":
    # colorama_init()
    # print(Fore.GREEN + "== Batch transaction manager ==" + Style.RESET_ALL)
    # parser = argparse.ArgumentParser(description='')
    # parser.add_argument("-s", "--servers", type=str, nargs='+', dest='servers', help='list of servers <addr>:<port>', required=False)
    # parser.add_argument('-c',"--consumer", type=str , dest='consumer_group', help='consumer group', required=True)
    # parser.add_argument("-t", "--topics", type=str , nargs='+', dest='topics', help='topis list', required=True)
    # args = parser.parse_args()

    bootstrap_servers = ['localhost:29092']
    topics = ["route_request_tpp", "weather_tp", "traffic_flow_tp"]
    topic_groups = {
        "traffic_flow_tp": "traffic_flow_consumer_group",
        "weather_tp": "weather_consumer_group",
        "route_request_tpp": "route_consumer_group"
    }
    consumer = Consumer()
    consumer.create_consumer(bootstrap_servers, topics, topic_groups)

    # Wait for the consumer threads to finish
    # try:
    #     while True:
    #         time.sleep(1)
    # except KeyboardInterrupt:
    #     pass

    # consumer.shutdown()
