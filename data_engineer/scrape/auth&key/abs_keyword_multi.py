import os
import json
import requests
import threading
from typing import List, Dict
from confluent_kafka import Producer


class ScopusDataScraper:
    def __init__(self, base_dataset_path: str, headers: Dict[str, str], kafka_topic: str, kafka_config: Dict[str, str]):
        self.base_dataset_path = os.path.abspath(os.path.expanduser(base_dataset_path))
        self.headers = headers
        self.kafka_topic = kafka_topic
        self.kafka_producer = Producer(kafka_config)
        self.lock = threading.Lock()

    def process_paths(self, paths: List[str] = None, num_threads: int = 4):
        if not paths:
            paths = [p for p in os.listdir(self.base_dataset_path)
                     if os.path.isdir(os.path.join(self.base_dataset_path, p)) and '20' in p]

        paths.sort()
        print(f"Found paths: {paths}")

        threads = []
        for path in paths:
            thread = threading.Thread(target=self._process_path, args=(path,))
            thread.start()
            threads.append(thread)

            if len(threads) >= num_threads:
                for t in threads:
                    t.join()
                threads = []

        for t in threads:
            t.join()

    def _process_path(self, path: str):
        full_path = os.path.join(self.base_dataset_path, path)
        files = os.listdir(full_path)

        for file in files:
            file_path = os.path.join(full_path, file)
            with open(file_path, 'r') as f:
                data = json.load(f)
                self._process_file(file, data)

    def _process_file(self, file: str, data: List[Dict]):
        for row in data:
            try:
                url = f"https://www.scopus.com/gateway/doc-details/documents/{row['eid']}"
                response = requests.get(url, headers=self.headers)

                if response.status_code != 200:
                    print(f"Failed to fetch data for {row['eid']}: HTTP {response.status_code}")
                    continue

                with self.lock:
                    row['abstract'] = response.json().get('abstract', [None])[0] if response.json().get('abstract') else None
                    row['author-keyword'] = response.json().get('authorKeywords', [])
                    self._send_to_kafka(row)

                    print(f"Processed and sent {row['eid']} to Kafka")

            except Exception as e:
                print(f"Error processing {row.get('eid', 'unknown')}: {e}")

    def _send_to_kafka(self, data: Dict):
        """Send processed data to Kafka topic."""
        try:
            self.kafka_producer.produce(
                self.kafka_topic,
                key=data['eid'],
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


# Example usage
def main():
    BASE_DATASET_PATH = './ScrapeDataset'
    HEADERS = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        "cookie": "your_cookie_here"
    }
    KAFKA_CONFIG = {
        "bootstrap.servers": "localhost:29092"
    }
    KAFKA_TOPIC = "auth_and_key"

    scraper = ScopusDataScraper(BASE_DATASET_PATH, HEADERS, KAFKA_TOPIC, KAFKA_CONFIG)
    scraper.process_paths(num_threads=10)


if __name__ == "__main__":
    main()
