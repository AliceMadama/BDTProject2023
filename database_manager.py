import pymongo
from colorama import Fore, Style


class DatabaseManager:
    '''
    Class to manage CRUD operations on a database.
    Before doing any operation, user should connect to database
    and select the proper collection to operate with
    '''

    def __init__(self, user="root", password="rootpassword", port="27017") -> None:
        self.__collection = None
        self.__database = None
        self.__client = pymongo.MongoClient(f"mongodb://{user}:{password}@127.0.0.1:{port}/", maxPoolSize=10000)

    def connect_to_database(self, database_name: str, collection_name="default_collection"):
        self.__database = self.__client[database_name]
        self.__collection = self.__database[collection_name]

    def select_collection(self, collection_name):
        # Check collection availability
        collections = self.__database.list_collection_names()
        if not collection_name in collections:
            print(
                Fore.YELLOW + '[DB] Warning: ' + Style.RESET_ALL + f"collection [{collection_name}] is empty. Will be created")
        self.__collection = self.__database[collection_name]

    # General Write operation
    def insert_document(self, document: list) -> bool:
        res = False
        if len(document) > 1:
            res = self.__collection.insert_many(document) is not None
        else:
            res = self.__collection.insert_one(document[0]) is not None
        return res

    def insert_document_to_collection(self, collection_name, documents):
        # Insert documents into the specified collection
        collection = self.__database[collection_name]
        collection.insert_many(documents)

    # General Read operation
    def execute_query(self, query):
        collections = self.__database.list_collection_names()
        # Check collection availability
        if self.__collection.name not in collections:
            print(
                Fore.YELLOW + '[DB] Warning: ' + Style.RESET_ALL + f"collection [{self.__collection.name}] does not exist")
            return []

        return self.__collection.find(*query)

    def update(self, query, newvalues):

        """
        Usage example:
            - query = { "address": "Valley 345" }
            - newvalues = { "$set": { "address": "Canyon 123" }}\n

            "$inc" can increase/decrease values
        """
        res = self.__collection.update_one(query, newvalues)
        return res

    def update_many(self, query, newvalues, filters):
        """
        Usage example:
            - query = { }
            - newvalues = { "$inc": { "money.$[]": 100 }}\n
            - filters check mongo docs

            "$inc" can increase/decrease values\n
            This allows for updating many documents matching the query
        """
        res = self.__collection.update_many(filter=query, update=newvalues, array_filters=filters)
        return res

    def delete_many(self, query) -> bool:
        """
        Deletes all documents given query. Usage:\n
        - query = {} -> delete all
        """
        res = self.__collection.delete_many(query)
        return res.deleted_count > 0

    def get_client(self):
        return self.__client

    def close_connection(self):
        if self.__client:
            self.__client.close()

    def get_collection_names(self):
        return self.__database.list_collection_names()



    def get_latest_data(self, collection_name):
        # Check collection availability
        collections = self.__database.list_collection_names()
        if collection_name not in collections:
            print(
                Fore.YELLOW + '[DB] Warning: ' + Style.RESET_ALL + f"collection [{collection_name}] does not exist")
            return None

        collection = self.__database[collection_name]
        latest_data = collection.find_one({}, sort=[("created_date", pymongo.DESCENDING)])
        return latest_data

    def find_one(self, query):
        return self.__collection.find_one(query)
if __name__ == "__main__":
    db_manager = DatabaseManager()
    db_manager.connect_to_database("traffic_management", "traffic_flow_tp")
    # db_manager.connect_to_database("traffic_management", "weather_tp")
    # db_manager.connect_to_database("traffic_management", "route_response_tp")
    # db_manager.connect_to_database("traffic_management", "customers")

    query_result = db_manager.execute_query({})
    for doc in query_result:
        print(doc)
    #
    # print("The latest -- ")
    # k = db_manager.get_latest_data("weather_tp")
    # print(k)

    db_manager.delete_many({})
