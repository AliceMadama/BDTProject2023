import datetime
import json
import uuid

from kafka import KafkaProducer
import pandas as pd
import time
from threading import Thread


class TrafficFlowProducer:
    def __init__(self, servers: list, topic: str, data_frame):
        self.producer = KafkaProducer(bootstrap_servers=servers,
                                      value_serializer=lambda m: json.dumps(m).encode('utf-8'))
        self.topic = topic
        self.data_frame = data_frame

    def send_traffic_event(self, traffic_event: dict) -> None:
        traffic_event['id'] = str(uuid.uuid4())
        traffic_event['created_date'] = datetime.datetime.now().isoformat()

        self.producer.send(self.topic, value=traffic_event)
        self.producer.flush()

    def generate_traffic_flow(self, interval: float = 1, duration: int = 10):
        start_time = time.time()
        end_time = start_time + duration
        while time.time() < end_time:
            threads = []
            for _, row in self.data_frame.iterrows():
                traffic_event = row.to_dict()
                t = Thread(target=self.send_traffic_event, args=(traffic_event,))
                t.start()
                threads.append(t)
                time.sleep(interval)  # Sleep for the specified interval

            # Wait for all threads to complete
            for t in threads:
                t.join()


if __name__ == "__main__":
    # Assuming you have a DataFrame called 'traffic_df' containing the traffic flow data
    traffic_df = pd.DataFrame({
        'timestamp': ['2000-01-01 00:15:00', '2000-01-01 00:30:00', '2000-01-01 00:45:00'],
        'RequestID': [2276, 2277, 2278],
        'Boro': ['Manhattan', 'Bronx', 'Queens'],
        'Vol': [231, 315, 189],
        'SegmentID': [32956, 32957, 32958],
        'WktGeom': ['POINT (986884.7 207042.2)', 'POINT (986884.8 207042.3)', 'POINT (986884.9 207042.4)'],
        'street': ['W/B UNION SQUARE E @ E 14 ST', 'W/B GRAND CONCOURSE @ E 167 ST', 'N/B 31 ST @ BROADWAY'],
        'fromSt': ['E 14 ST', 'E 167 ST', '31 ST'],
        'toSt': ['4 AV', 'River Ave', 'Broadway'],
        'Direction': ['WB', 'WB', 'NB']
    })

    bootstrap_servers = ['localhost:29092']
    topic = 'traffic_flow_tp'

    traffic_producer = TrafficFlowProducer(bootstrap_servers, topic, traffic_df)
    traffic_producer.generate_traffic_flow(interval=1, duration=30)  # Send three events every 1 second for 10 seconds
