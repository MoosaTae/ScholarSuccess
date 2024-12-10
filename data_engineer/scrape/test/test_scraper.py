import json
import time
import random
from confluent_kafka import Producer

class TestScraper:
    def __init__(self, kafka_bootstrap_servers: str, topics_and_files: dict):
        self.topics_and_files = topics_and_files
        self.producer = Producer({
            'bootstrap.servers': kafka_bootstrap_servers
        })

    def _delivery_report(self, err, msg):
        if err is not None:
            print(f"Delivery failed for record {msg.key()}: {err}")
        else:
            print(f"Record produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def simulate_scrape(self, count=10, delay=1):
        """
        Simulate a scraping process by generating random data 
        and sending it to multiple Kafka topics.
        """
        for topic, file_name in self.topics_and_files.items():
            for i in range(count):
                # Generate dummy data
                data = {
                    "id": i + 1,
                    "topic": topic,
                    "file": file_name,
                    "content": f"Sample content for {topic} record {i + 1}",
                    "timestamp": time.time()
                }

                # Send data to Kafka
                self.producer.produce(
                    topic, 
                    key=str(data["id"]), 
                    value=json.dumps(data), 
                    callback=self._delivery_report
                )

                # Poll Kafka for events
                self.producer.poll(0)
                time.sleep(delay)

        # Flush outstanding messages
        self.producer.flush()

def main():
    kafka_bootstrap_servers = "localhost:29092"  # Change if needed

    topics_and_files = {
        "aff": "aff.txt",
        "auth_list": "auth_list.txt",
        "auth_stat": "auth_stat.txt",
        "auth_and_key": "auth_and_key.txt",
        "paper": "paper.txt",
    }

    scraper = TestScraper(kafka_bootstrap_servers, topics_and_files)
    scraper.simulate_scrape(count=5, delay=2)

if __name__ == "__main__":
    main()
