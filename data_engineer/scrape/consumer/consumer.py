from confluent_kafka import Consumer, KafkaException
import json
import threading
import os

def consume_data(topic: str, output_file: str, kafka_config: dict):
    consumer = Consumer({
        'bootstrap.servers': kafka_config['bootstrap.servers'],
        'group.id': f'{topic}_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([topic])

    try:
        # Ensure directory exists if specified
        if os.path.dirname(output_file):
            os.makedirs(os.path.dirname(output_file), exist_ok=True)

        with open(output_file, 'a', buffering=1) as f:  # Use line buffering
            print(f"Listening on {topic}...")
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                data = json.loads(msg.value().decode('utf-8'))
                f.write(json.dumps(data, indent=4) + '\n')
                f.flush()  # Ensure immediate writing to the file
                print(f"Wrote message from {topic} to {output_file}")

    except KeyboardInterrupt:
        print(f"Stopping consumer for {topic}...")
    finally:
        consumer.close()

if __name__ == "__main__":
    kafka_config = {
        'bootstrap.servers': 'localhost:29092'
    }

    OUTPUT_DIR = os.path.abspath('./output')

    topics_and_files = {
        "aff": os.path.join(OUTPUT_DIR, "aff.txt"),
        "auth_list": os.path.join(OUTPUT_DIR, "auth_list.txt"),
        "auth_stat": os.path.join(OUTPUT_DIR, "auth_stat.txt"),
        "auth_and_key": os.path.join(OUTPUT_DIR, "auth_and_key.txt"),
        "paper": os.path.join(OUTPUT_DIR, "paper.txt"),
    }

    # Start consumers in separate threads
    threads = []
    for topic, output_file in topics_and_files.items():
        t = threading.Thread(target=consume_data, args=(topic, output_file, kafka_config))
        t.start()
        threads.append(t)

    # Wait for all threads to complete
    for t in threads:
        t.join()
