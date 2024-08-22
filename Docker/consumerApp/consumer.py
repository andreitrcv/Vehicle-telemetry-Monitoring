from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import os
import json
import time
from datetime import datetime

# Kafka broker host and port
kafka_broker = os.getenv('KAFKA_BOOTSTRAP_SERVERS')

# Kafka topic to consume from
topic = 'telemetry'
# Consumer group id
group_id = 'test_consumer_group'

# Output file path
output_file = 'telemetry_messages.jsonl'  # Using .jsonl format to handle line-delimited JSON

# Kafka consumer configuration
conf = {
    'bootstrap.servers': kafka_broker,
    'group.id': group_id,
    'auto.offset.reset': 'earliest'  # Read from the beginning of the topic
}

def kafka_consumer():
    # Create Kafka Consumer instance
    consumer = Consumer(conf)

    try:
        # Subscribe to topic
        consumer.subscribe([topic])

        with open(output_file, 'a') as file:
            while True:
                # Poll for messages
                msg = consumer.poll(timeout=0.1)  # Timeout in seconds
                current_time = datetime.now()
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(msg.error())
                        break

                # Decode the message and parse JSON
                message_str = msg.value().decode('utf-8')
                try:
                    message_json = json.loads(message_str)
                except json.JSONDecodeError as e:
                    print(f"Failed to decode JSON: {e}")
                    continue

                # Extract the timestamp from the JSON message
                if 'timestamp' in message_json:
                    timestamp_str = message_json['timestamp']
                    try:
                        # Convert the timestamp string to a datetime object
                        log_timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')

                        # Calculate the time difference (latency)
                        time_difference = current_time - log_timestamp
                    except ValueError as e:
                        print(f"Timestamp parsing error: {e}")
                        continue
                else:
                    print("Timestamp not found in message.")
                    continue

                # Write the JSON message and latency to file
                message_json['latency'] = str(time_difference)
                file.write(json.dumps(message_json) + '\n')

    except KeyboardInterrupt:
        sys.stderr.write('Aborted by user\n')

    finally:
        # Close Kafka Consumer
        consumer.close()

if __name__ == '__main__':
    time.sleep(30)  # Delay startup if needed
    kafka_consumer()

