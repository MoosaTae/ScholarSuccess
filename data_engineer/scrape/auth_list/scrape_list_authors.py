import os
import json
import time
import random
import logging
from typing import List, Dict, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import tqdm
from confluent_kafka import Producer

class AbstractFetcher:
    def __init__(
        self, 
        api_key: str, 
        data: List[Dict], 
        kafka_topic: str, 
        kafka_config: Dict[str, str], 
        max_workers: int = 3, 
        max_retries: int = 3
    ):
        self.api_key = api_key
        self.data = data
        self.kafka_topic = kafka_topic
        self.kafka_producer = Producer(kafka_config)
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.session = requests.Session()
        self._setup_logging()

    def _setup_logging(self) -> None:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s: %(message)s',
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger(__name__)

    def _make_api_request(self, url: str, doi: str) -> Optional[Dict]:
        headers = {
            'X-ELS-APIKey': self.api_key,
            'Accept': 'application/json'
        }

        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, headers=headers, timeout=10)
                
                if response.status_code == 429:
                    wait_time = min(60, (2 ** attempt) + random.uniform(0, 1))
                    self.logger.warning(f"Rate limited. Waiting {wait_time} seconds for DOI {doi}")
                    time.sleep(wait_time)
                    continue
                
                if response.status_code != 200:
                    self.logger.error(f"Unexpected status code {response.status_code} for DOI {doi}")
                    return None
                
                return response.json()
            
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request error for DOI {doi} (Attempt {attempt + 1}): {e}")
                time.sleep(1)
        
        return None

    def _parse_abstract_data(self, data: Dict, doi: str) -> Optional[Dict]:
        try:
            response = data.get('abstracts-retrieval-response', {}).get('coredata', {})
            creator = response.get('dc:creator')

            if isinstance(creator, dict):
                creator = creator.get('author')
            elif isinstance(creator, list):
                creator = [a.get('author', a) for a in creator]

            parsed_data = {
                'doi': doi,
                'author': creator,
                'source_id': response.get('source-id')
            }

            self._send_to_kafka(parsed_data)
            return parsed_data
        
        except Exception as e:
            self.logger.error(f"Data parsing error for DOI {doi}: {e}")
            return None

    def _send_to_kafka(self, data: Dict):
        try:
            self.kafka_producer.produce(
                self.kafka_topic,
                key=data['doi'],
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

    def _process_paper(self, paper: Dict) -> Optional[Dict]:
        try:
            doi = paper.get('prism:doi')
            url = paper.get('prism:url')

            if not doi or not url:
                self.logger.warning(f"Missing DOI or URL for paper: {paper}")
                return None

            data = self._make_api_request(url, doi)
            if data is None:
                self.logger.warning(f"No data retrieved for DOI: {doi}")
                return None

            return self._parse_abstract_data(data, doi)
        
        except Exception as e:
            self.logger.error(f"Processing error for paper {doi}: {e}")
            return None

    def fetch_abstracts(self) -> List[Dict]:
        self.logger.info(f"Processing {len(self.data)} papers")
        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            progress_bar = tqdm.tqdm(total=len(self.data), desc="Fetching Abstracts", unit="paper")
            futures = {executor.submit(self._process_paper, paper): paper for paper in self.data}

            for future in as_completed(futures):
                result = future.result()
                progress_bar.update(1)
                if result:
                    results.append(result)

            progress_bar.close()
        
        self.logger.info(f"Processed {len(results)} papers successfully")
        return results


def main():
    try:
        example_data = [
            {
                "prism:doi": "10.1109/Group4.2013.6644459", 
                "prism:url": "https://api.elsevier.com/content/abstract/scopus_id/84891142203"
            },
            {
                "prism:doi": "10.1109/INTECH.2013.6653713", 
                "prism:url": "https://api.elsevier.com/content/abstract/scopus_id/84891142185"
            }
        ]
        
        api_key = os.environ.get('SCOPUS_API_KEY', '72d3da80128e29b337ae7f405f2cddf8')
        kafka_config = {"bootstrap.servers": "localhost:29092"}
        kafka_topic = "auth_list"

        start_time = time.time()
        
        fetcher = AbstractFetcher(
            api_key=api_key,
            data=example_data,
            kafka_topic=kafka_topic,
            kafka_config=kafka_config,
            max_workers=3,
            max_retries=3
        )

        updated_data = fetcher.fetch_abstracts()
        end_time = time.time()

        print(f"Total processing time: {end_time - start_time:.2f} seconds")
        print(json.dumps(updated_data, indent=2))

    except Exception as e:
        logging.error(f"Critical error in main execution: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
