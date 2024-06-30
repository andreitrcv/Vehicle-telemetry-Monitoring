import time
import random
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

producer = Producer(kafka_config)


# Metrics and their descriptions
metrics = {
    "Engine Speed (RPM)": None,
    "Engine Temperature": None,
    "Oil Pressure": None,
    "Fuel Consumption": None,
    "Exhaust Gas Temperature": None,
    "Battery Status": None,
    "Air Filter Condition": None
}

# Function to generate and print messages with faker data
def generate_mock_message():
    vehicle_id = busID
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    
    # Generate random values for metrics
    metrics["Engine Speed (RPM)"] = str(random.randint(1000, 6000))
    metrics["Engine Temperature"] = f"{random.randint(50, 100)}°C"
    metrics["Oil Pressure"] = f"{random.randint(30, 80)} psi"
    metrics["Fuel Consumption"] = f"{round(random.uniform(8, 15), 1)} L/100km"
    metrics["Exhaust Gas Temperature"] = f"{random.randint(300, 600)}°C"
    metrics["Battery Status"] = f"{round(random.uniform(11.5, 13.5), 1)}V"

    # Create message with all metrics
    message_parts = []
    for metric_key, metric_value in metrics.items():
        message_parts.append(f"[{metric_key}] [{metric_value}]")
    
    # Join all parts into a single message
    message = f"[{vehicle_id}] [{timestamp}] - " + " ".join(message_parts)
    
    # Produce the message to Kafka with specified partition
    producer.produce("telemetry", key=vehicle_id, value=message.encode('utf-8'))

    # Flush the producer to ensure the message is sent immediately
    producer.flush()



# Timer function to update Air Filter Condition every 5 hours
def update_air_filter_condition():
    while True:
        metrics["Air Filter Condition"] = random.choice(["Clean", "Moderate", "Dirty"])
        # Sleep for 5 hours
        time.sleep(18000)

def shutdown_hook():
    producer.flush(timeout=5)  # Wait for up to 5 seconds for the messages to be delivered
    producer.close()

atexit.register(shutdown_hook)

# Main loop to generate messages every N seconds
def main(interval_seconds):
    # Start a thread to update Air Filter Condition periodically
    air_filter_thread = threading.Thread(target=update_air_filter_condition)
    air_filter_thread.daemon = True  # Daemonize the thread so it terminates when main program exits
    air_filter_thread.start()

    # Main loop to generate messages
    while True:
        generate_mock_message()
        time.sleep(interval_seconds)

if __name__ == "__main__":
    interval_seconds = 0.05 # Change this value to set the interval in seconds
    time.sleep(60)
    producer = Producer(kafka_config)
    main(interval_seconds)
