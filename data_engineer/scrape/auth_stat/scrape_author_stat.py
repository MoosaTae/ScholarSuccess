import json
import time
import random
import logging
import sys
from typing import List, Dict, Optional
from confluent_kafka import Producer


class AuthorScraper:
    def __init__(
        self, 
        data: List[Dict], 
        kafka_topic: str, 
        kafka_config: Dict[str, str],
        max_workers: int = 3, 
        rate_limit_delay: tuple = (1, 3)
    ):
        self.data = data
        self.kafka_topic = kafka_topic
        self.kafka_producer = Producer(kafka_config)
        self.max_workers = max_workers
        self.rate_limit_delay = rate_limit_delay
        self._setup_logging()

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s: %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        self.logger = logging.getLogger(__name__)

    def _generate_mock_metrics(self, author_id: str) -> Dict:
        """
        Generate mock author metrics for testing.
        """
        return {
            'author_id': author_id,
            'citations': random.randint(100, 1000),
            'documents': random.randint(10, 100),
            'h_index': random.randint(1, 50),
        }

    def scrape_author_data(self, author: Dict) -> Optional[Dict]:
        """
        Mock method to simulate scraping author data.
        """
        if not isinstance(author, dict):
            self.logger.warning(f"Invalid author data type: {type(author)}")
            return None

        author_id = author.get('@auid')
        if not author_id:
            self.logger.warning("Skipping author with invalid ID")
            return None

        try:
            time.sleep(random.uniform(*self.rate_limit_delay))  # Simulate delay
            mock_metrics = self._generate_mock_metrics(author_id)
            self._send_to_kafka(mock_metrics)
            return mock_metrics
        except Exception as e:
            self.logger.error(f"Error generating mock data for AUID {author_id}: {e}")
            return None

    def _send_to_kafka(self, data: Dict):
        """
        Send data to Kafka topic.
        """
        try:
            self.kafka_producer.produce(
                self.kafka_topic,
                key=data['author_id'],
                value=json.dumps(data).encode('utf-8'),
                callback=self._delivery_report
            )
            self.kafka_producer.flush()
        except Exception as e:
            self.logger.error(f"Failed to send data to Kafka: {e}")

    def _delivery_report(self, err, msg):
        if err:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            self.logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def run(self):
        results = []
        for author in self.data:
            result = self.scrape_author_data(author)
            if result:
                results.append(result)
        return results


def main():
    try:
        example_data = [
            {"@auid": "57113534800"},
            {"@auid": "57217590099"}
        ]
        KAFKA_CONFIG = {
            "bootstrap.servers": "localhost:29092"
        }
        KAFKA_TOPIC = "auth_stat"

        scraper = AuthorScraper(
            data=example_data,
            kafka_topic=KAFKA_TOPIC,
            kafka_config=KAFKA_CONFIG,
            max_workers=5,
            rate_limit_delay=(1, 3)
        )

        results = scraper.run()
        print(json.dumps(results, indent=2))

    except Exception as e:
        logging.error(f"Critical Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
