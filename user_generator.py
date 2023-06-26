from __future__ import annotations

import random

from database_manager import DatabaseManager
from colorama import Fore, Style
from random import randint
import uuid
from faker import Faker


class Customer():
    """
    Class of a customer
    """

    def __init__(self, id: str,
                 first_name: str,
                 last_name: str,
                 email: str,
                 user_name: str) -> None:
        self.id = id
        self.first_name = first_name
        self.last_name = last_name
        self.user_name = user_name
        self.email = email

    @staticmethod
    def from_repr(data: dict) -> Customer:
        return Customer(data['id'], data['first_name'], data['last_name'], data['email'], data['user_name'])

    @staticmethod
    def to_repr(customer: Customer) -> dict:
        person = {
            'id': customer.id,
            'first_name': customer.first_name,
            'last_name': customer.last_name,
            'email': customer.email,
            'username': f"{customer.first_name.lower()}{customer.last_name.lower()}{random.randint(10, 99)}"
        }
        return person


class CustomersGenerator():
    def __init__(self, database_name="traffic_management", collection_name="customers", default_customers=10) -> None:
        """
        Creates some default customers if not already present into database
        """
        # Connection to database
        self.__db_manager = DatabaseManager()
        self.__db_manager.connect_to_database(database_name, collection_name)
        self.__db_manager.select_collection(collection_name)
        res = list(self.__db_manager.execute_query({}))

        # Check if default data of customers is available
        if len(res) == 0:
            print(
                Fore.YELLOW + 'Warning: ' + Style.RESET_ALL + f"default [{collection_name}] collection is empty. Downloading...")
            customers = self.generate(default_customers)

            # Saving customers to database
            self.__db_manager.insert_document([Customer.to_repr(x) for x in customers])

    def generate(self, quantity=10) -> list[Customer]:
        """
        Generates random customers e.g Mario Rossi etc. and extends the customer's database collection
        """
        person_list = []
        person_list.extend(
            self.__generate_customers(quantity=quantity))
        return person_list

    def __generate_customers(self, quantity=10) -> list[Customer]:
        fake = Faker()
        # Generate multiple fake user profiles

        customers = []
        for i in range(quantity):
            customer = Customer(
                str(uuid.uuid4()),
                fake.first_name(),
                fake.last_name(),
                fake.email(),
                fake.user_name()
            )
            customers.append(customer)
        return customers

    def get_customers(self, limit="") -> list[dict]:
        '''
        Retuns the customers from database as Jsons
        '''
        res = self.__db_manager.execute_query([{}, {"_id": 0, }])
        res = list(res.limit(limit if limit else 0))
        if len(res) == 0:
            return []
        return res
