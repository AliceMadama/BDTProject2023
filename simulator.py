import os
import subprocess
import time


def main():
    route_request_sender = 'kafka_producer_route.py'
    traffic_sender = 'kafka_producer_traffic.py'
    weather_sender = 'kafka_producer_weather.py'
    global_consumer = 'kafka_consumer.py'

    nrt_processor = 'nrt_stream_process.py'
    response_d = 'response_distribute.py'

    # Set the number of trials and simulation trial total time
    NUM_TRIALS = 1
    SIMULATION_TRIAL_TOTAL_TIME = 3000

    # Set the number of customers threads
    CUSTOMERS_THREADS = 2

    # Redirect output to the null device
    devnull = open(os.devnull, 'w')

    for _ in range(NUM_TRIALS):
        # Start the script in a separate process

        # Execute consumers:
        consumer = subprocess.Popen(['python', global_consumer])

        # Execute producers
        traffic = subprocess.Popen(['python', traffic_sender])
        weather = subprocess.Popen(['python', weather_sender])
        route = subprocess.Popen(['python', route_request_sender], stdout=devnull, stderr=devnull)

        # Execute near realtime processor
        nrt = subprocess.Popen(['python', nrt_processor])
        response = subprocess.Popen(['python', response_d])

        # Wait for the simulation trial time
        time.sleep(SIMULATION_TRIAL_TOTAL_TIME)

        # Terminate the process
        traffic.terminate()
        traffic.wait()
        weather.terminate()
        weather.wait()
        route.terminate()
        route.wait()
        consumer.terminate()
        consumer.wait()
        nrt.terminate()
        nrt.wait()
        response.terminate()
        response.wait()

        # Wait for a brief period before starting the next trial
        time.sleep(1)


if __name__ == '__main__':
    main()
