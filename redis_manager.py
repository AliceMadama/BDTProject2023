import redis
from colorama import Fore, Style
import json


class RedisManager:
    """
    Wrapper for Redis client
    """

    def __init__(self, host="127.0.0.1", port="6379", password="redis_password") -> None:
        self.__instance = redis.Redis(
            host=host,
            port=port,
            # password=password
        )

    def get_instance(self):
        return self.__instance

    def test_connection(self) -> bool:
        return self.__instance.ping()

    def get_data_from_cache(self, key):
        # Check if the data exists in the cache
        if self.__instance.exists(key):
            # Retrieve the data from the cache
            data = self.__instance.get(key)
            return data.decode('utf-8')
        else:
            # Data does not exist in the cache
            return None

    def set_data_in_cache(self, key, data, expiration=None):
        # Set the data in the cache
        self.__instance.set(key, data)

        # Optionally set an expiration time for the cached data
        if expiration:
            self.__instance.expire(key, expiration)

    def delete_data_from_cache(self, key):
        # Delete the data from the cache
        self.__instance.delete(key)


if __name__ == "__main__":
    # Example usage
    db_manager = RedisManager().get_instance()
    res = db_manager.get("route_request_tp:3622cf0e-eaa6-44ac-a633-97f926e106b8")


    print(res)
    res = db_manager.get("weather_tp")
    print(res)
    res = db_manager.get("traffic_flow_tp")
    print(res)
    # read_transactions(db_manager, list_name="weather_tp")
    # read_transactions(db_manager, list_name="traffic_flow_tp")
    # db_manager.delete("route_requests_tp")
