import os
import json
import requests
import threading
from queue import Queue
from confluent_kafka import Producer

class AffiliationDataProcessor:
    def __init__(self, input_file, output_file, headers, kafka_topic, kafka_config):
        self.input_file = input_file
        self.output_file = output_file
        self.headers = headers
        self.data = []
        self.queue = Queue()
        self.lock = threading.Lock()
        self.kafka_topic = kafka_topic
        self.kafka_producer = Producer(kafka_config)
        self.count = 0

    def load_data(self):
        with open(self.input_file, 'r') as f:
            self.data = json.load(f)

    def fetch_affiliation_data(self):
        while not self.queue.empty():
            row = self.queue.get()
            affid = row['@affid']
            url = f"https://www.scopus.com/gateway/organisation-profile-api/organizations/{affid}"

            try:
                response = requests.get(url, headers=self.headers)
                if response.status_code != 200:
                    print(f"Failed to fetch data for @affid: {affid} (Status: {response.status_code})")
                    return

                req_json = response.json()
                with self.lock:
                    row['metrics'] = req_json.get('metrics', {})
                    row['preferredName'] = req_json.get('preferredName', None)
                    row['nameVariants'] = req_json.get('nameVariants', None)
                    row['address'] = req_json.get('address', None)
                    row['hierarchyIds'] = req_json.get('hierarchyIds', None)
                    row['contact'] = req_json.get('contact', None)
                    self.count += 1

                self._send_to_kafka(row)

            except Exception as e:
                print(f"Error fetching data for @affid: {affid}: {e}")
            finally:
                print(f"Processed Count: {self.count}")
                self.queue.task_done()

    def process_data(self, num_threads=5):
        for row in self.data:
            self.queue.put(row)

        threads = []
        for _ in range(num_threads):
            thread = threading.Thread(target=self.fetch_affiliation_data)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    def save_data(self):
        with open(self.output_file, 'w') as f:
            json.dump(self.data, f, indent=4)

    def _send_to_kafka(self, data):
        try:
            self.kafka_producer.produce(
                self.kafka_topic,
                key=str(data['@affid']),
                value=json.dumps(data).encode('utf-8'),
                callback=self._delivery_report
            )
            self.kafka_producer.flush()
        except Exception as e:
            print(f"Failed to send data to Kafka: {e}")

    def _delivery_report(self, err, msg):
        if err:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def run(self, num_threads=5):
        self.load_data()
        self.process_data(num_threads=num_threads)
        self.save_data()


if __name__ == "__main__":
    headers = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        "cookie": "your_cookie_here"  # Update with a valid cookie
    }

    kafka_config = {
        "bootstrap.servers": "localhost:29092"
    }
    kafka_topic = "aff"

    processor = AffiliationDataProcessor(
        './all_affid_list.json',
        './all_affid_list_add.json',
        headers,
        kafka_topic,
        kafka_config
    )

    processor.run(num_threads=10)
