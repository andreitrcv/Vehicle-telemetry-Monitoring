import time
from datetime import datetime
import random
import json
import threading  # For Air Filter Condition special generation
import os
import atexit
from confluent_kafka import Producer

# Fetch Kafka broker address from environment variable
kafka_broker = os.getenv('KAFKA_BOOTSTRAP_SERVERS')

# Fetch HOSTNAME from environment variable to use it as message key
busID = os.getenv('HOSTNAME')

if kafka_broker is None:
    raise ValueError("KAFKA_BROKER_ADDRESS environment variable is not set")

# Kafka producer configuration
kafka_config = {
    'bootstrap.servers': kafka_broker,
    # Other configurations if needed..
}

# For performance throughput
message_count = 0
start_time = time.time()

producer = Producer(kafka_config)


# Metrics and their descriptions
metrics = {
    "Engine_Speed_RPM": None,
    "Engine_Temperature": None,
    "Oil_Pressure": None,
    "Fuel_Consumption": None,
    "Exhaust_Gas_Temperature": None,
    "Battery_Status": None,
    "Air_Filter_Condition": None
}

# Function to generate and print messages with faker data
def generate_mock_message():
    global message_count
    vehicle_id = busID
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    # Generate random values for metrics with realistic behavior
    metrics["Engine_Speed_RPM"] = max(0, min(int(random.gauss(3000, 500)), 8000))  # Normal distribution, with mean at 3000 RPM, SD=500
    metrics["Engine_Temperature"] = max(0, min(int(random.gauss(80, 10)), 120))  # Mean around 80°C, SD=10, limit at 120°C
    metrics["Oil_Pressure"] = max   (0, min(int(random.gauss(60, 10)), 100))  # Mean around 60 psi, SD=10, limit at 100 psi
    metrics["Fuel_Consumption"] = round(max(0, min(random.gauss(10, 1), 20)), 1)  # Mean of 10 L/100km, SD=1, max 20 L/100km
    metrics["Exhaust_Gas_Temperature"] = max(200, min(int(random.gauss(400, 50)), 800))  # Mean at 400°C, SD=50, limits between 200 and 800°C
    metrics["Battery_Status"] = round(max(10, min(random.gauss(12.5, 0.5), 14)), 1)  # Mean around 12.5V, SD=0.5, limit between 10V and 14V

    # Introduce rare abnormal values
    if random.random() < 0.001:  # 0.1% chance of an outlier
        metrics["Engine_Speed_RPM"] = random.randint(8000, 9000)  # Spike in RPM
        metrics["Engine_Temperature"] = random.randint(120, 150)  # Overheating
        metrics["Oil_Pressure"] = random.randint(10, 30)  # Low oil pressure
        metrics["Fuel_Consumption"] = round(random.uniform(20, 30), 1)  # High fuel consumption
        metrics["Exhaust_Gas_Temperature"] = random.randint(800, 1000)  # Extreme exhaust temperature
        metrics["Battery_Status"] = round(random.uniform(9, 10), 1)  # Battery draining


    # Include vehicle_id and timestamp in the message
    message = {
        "vehicle_id": vehicle_id,
        "timestamp": timestamp,
        **metrics
    }
    
    # Serialize the message to JSON
    message_json = json.dumps(message)
    
    # Produce the message to Kafka
    producer.produce("telemetry", key=vehicle_id, value=message_json.encode('utf-8'))
    message_count += 1
    

# Timer function to update Air Filter Condition every 5 hours
def update_air_filter_condition():
    while True:
        metrics["Air_Filter_Condition"] = random.choices(
            ["Clean", "Moderate", "Dirty"],
            weights=[90, 9, 1]  # Most often Clean, rarely Dirty
        )[0]
        # Sleep for 5 hours
        time.sleep(18000)

def shutdown_hook():
    producer.flush(timeout=5)  # Wait for up to 5 seconds for the messages to be delivered
    producer.close()

atexit.register(shutdown_hook)

def print_throughput():
        global message_count, start_time
        elapsed_time = time.time() - start_time
        if elapsed_time > 0:
            production_rate = message_count / elapsed_time
            print(f"Production rate: {production_rate:.2f} messages/second")
        message_count = 0
        start_time = time.time()


# Main loop to generate messages every N seconds
def main(interval_seconds):
    # Start a thread to update Air Filter Condition periodically
    air_filter_thread = threading.Thread(target=update_air_filter_condition)
    air_filter_thread.daemon = True  # Daemonize the thread so it terminates when main program exits
    air_filter_thread.start()

    # Main loop to generate messages
    while True:
        generate_mock_message()
        if message_count % 1000 == 0:
            print_throughput()
        time.sleep(interval_seconds)

if __name__ == "__main__":
    interval_seconds = 0.05 # Change this value to set the interval in seconds
    time.sleep(60)
    producer = Producer(kafka_config)
    main(interval_seconds)
