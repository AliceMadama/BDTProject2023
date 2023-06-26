import datetime
import json
import logging
import uuid

from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
import os
from pyspark.sql.types import StringType, ArrayType, StructType, StructField

from database_manager import DatabaseManager
from open_route_manager import DirectionsAPI
from redis_manager import RedisManager


def main():
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
    os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'

    # Set log level to ERROR for KafkaDataConsumer and urllib3
    logging.getLogger("org.apache.spark.streaming.kafka010.KafkaDataConsumer").setLevel(logging.ERROR)
    logging.getLogger("urllib3").setLevel(logging.ERROR)

    spark = SparkSession.builder.appName("KafkaStructuredStreamingExample").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    bootstrap_servers = ['localhost:29092']
    topic = 'route_request_tpp'

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", *bootstrap_servers) \
        .option("subscribe", topic) \
        .load()

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    df = df.selectExpr("CAST(value AS STRING)")

    # Define the schema for parsing route request JSON data
    schema = StructType([
        StructField("id", StringType()),
        StructField("src", StructType([
            StructField("longitude", StringType()),
            StructField("latitude", StringType())
        ])),
        StructField("dst", StructType([
            StructField("longitude", StringType()),
            StructField("latitude", StringType())
        ]))
    ])

    # redis_manager = RedisManager().get_instance()

    root_manager = DirectionsAPI()

    def my_udf(value1, value2):
        route_request = {
            'src': [value1["longitude"], value1["latitude"]],
            'dst': [value2["longitude"], value2["latitude"]],
        }

        response = root_manager.get_directions(route_request)

        # # Check if the traffic flow data is cached in Redis
        redis_manager = RedisManager().get_instance()
        mongo_db_manager = DatabaseManager()

        #################################
        traffic_flow_key = "traffic_flow_tp"
        mongo_db_manager.connect_to_database("traffic_management", traffic_flow_key)
        traffic_flow_data = redis_manager.get(traffic_flow_key)

        if traffic_flow_data:
            print("Weather data fetched from Redis.")
            traffic_flow_data = json.loads(traffic_flow_data.decode('utf-8'))
        else:
            print("Weather data fetched from Mongo.")
            traffic_flow_data = mongo_db_manager.get_latest_data(traffic_flow_key)
            # Cache the traffic flow data in Redis
            # redis_manager.set(traffic_flow_key, json.dumps(traffic_flow_data))

        #################################
        weather_key = "weather_tp"
        mongo_db_manager.connect_to_database("traffic_management", weather_key)
        weather_data = redis_manager.get(weather_key)

        if weather_data:
            print("Weather data fetched from Redis.")
            weather_data = json.loads(weather_data.decode('utf-8'))
        else:
            print("Weather data fetched from Mongo.")
            # Fetch weather data from MongoDB using a key
            weather_data = mongo_db_manager.get_latest_data(weather_key)
            # Cache the weather data in Redis
            # redis_manager.set(weather_key, json.dumps(weather_data))

        print(response)

        # response = json.loads(response) #to json
        # print("111")
        # print(traffic_flow_data["Vol"])
        # print(weather_data["temperature"])
        # print(response["type"])
        response['congestion_level'] = traffic_flow_data["Vol"]  # after ml
        response['weather_condition'] = weather_data["temperature"]

        return json.dumps(response)

    my_udf_spark = udf(my_udf)

    processed_stream = df.select(from_json(col("value"), schema).alias("route_request")) \
        .select("route_request.id",
                my_udf_spark("route_request.dst", "route_request.src").alias("processed_dst"))

    # Print the received data
    query = processed_stream.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    def send_to_kafka(record):

        # Save the processed stream into Kafka
        kafka_producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

        print("---", record.asDict())
        value = json.dumps(record.asDict(), ensure_ascii=False).encode('utf-8')
        print("===", value)
        kafka_producer.send('route_response_tp', value=value)
        #
        # producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        # value = json.dumps(record).encode('utf-8')
        # print(value)
        # producer.send("route_response_tp", value)

    query = processed_stream.writeStream \
        .outputMode("append") \
        .foreach(send_to_kafka) \
        .start()

    # Await termination
    query.awaitTermination()


if __name__ == "__main__":
    main()
