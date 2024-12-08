import os
import sys
import json
import time
import random
import logging
import pandas as pd
import concurrent.futures
from datetime import datetime


import backoff
from tqdm.notebook import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

input_file = 'chunk_kink.json'

class AuthorScraper:
    def __init__(self,
                 input_dir,
                 output_dir='output',
                 log_dir='logs',
                 max_workers=3,
                 rate_limit_delay=(1, 3),
                 retry_max_attempts=3, 
                 failed_auids_threshold=100):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.log_dir = log_dir
        self.max_workers = max_workers
        self.rate_limit_delay = rate_limit_delay
        self.retry_max_attempts = retry_max_attempts

        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)

        self._setup_logging()

        self.failed_auids = []
        self.failed_auids_set = set()
        self.failed_auids_threshold = failed_auids_threshold
        self.failed_auids_file = os.path.join(self.output_dir, 'failed_auids.json')

    def _setup_logging(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(self.log_dir, f'author_scraper_{timestamp}.log')

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s: %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _log_failed_auid(self, author):
        try:
            print(author)
            # Ensure the failed_auid is a dictionary with @auid key
            if isinstance(author, dict):
                failed_auid_entry = {'@auid': author.get('@auid', 'UNKNOWN')}
            else:
                failed_auid_entry = {'@auid': str(author)}
            
            if failed_auid_entry['@auid'] not in self.failed_auids_set:
                self.failed_auids.append(failed_auid_entry)
                self.failed_auids_set.add(failed_auid_entry['@auid'])
                
                # Write when threshold is reached
                if len(self.failed_auids) >= self.failed_auids_threshold:
                    with open(self.failed_auids_file, 'w') as f:
                        json.dump(self.failed_auids, f, indent=2)
                    self.logger.warning(f"Logged {len(self.failed_auids)} failed AUIDs")
        
        except Exception as log_error:
            self.logger.error(f"Error logging failed AUID: {log_error}")

    def create_driver(self):
        chrome_options = Options()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--log-level=3')

        service = Service(ChromeDriverManager().install())
        try:
            driver = webdriver.Chrome(service=service, options=chrome_options)
        except Exception as e:
            self.logger.error(f"WebDriver initialization error: {e}")
            try:
                driver = webdriver.Chrome(options=chrome_options)
            except Exception as fallback_error:
                self.logger.critical(f"Failed to initialize WebDriver: {fallback_error}")
                raise

        driver.implicitly_wait(10)
        return driver

    @staticmethod
    def str2int(s):
        try:
            return int(str(s).replace(',', ''))
        except (ValueError, AttributeError, TypeError):
            return 0

    def scrape_author_data(self, author):
        # Validate input
        if not isinstance(author, dict):
            self.logger.warning(f"Invalid author data type: {type(author)}")
            self._log_failed_auid(author)
            return None

        # Ensure required keys exist
        author_id = author.get('@auid', 'UNKNOWN')

        if not author_id or author_id == 'UNKNOWN':
            self.logger.warning(f"Skipping author with invalid ID: {author}")
            self._log_failed_auid(author)
            return None

        driver = None
        try:
            # Randomized delay
            time.sleep(random.uniform(1, 3))

            driver = self.create_driver()
            url = f'https://www.scopus.com/authid/detail.uri?authorId={author_id}'
            driver.get(url)

            # Extended diagnostics for page load and element detection
            try:
                list_matric = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".MetricSection-module__s8lWB"))
                )

                # Capture full page source for debugging
                metrics_text = list_matric.text

                self.logger.info(f"Metrics text for auid {author_id}: {metrics_text}")

                # Split metrics with additional safety
                data = metrics_text.split("\n")

                # Comprehensive safeguards for data extraction
                def safe_extract(data_list, index, default=0):
                    try:
                        return self.str2int(data_list[index]) if len(data_list) > index else default
                    except Exception as e:
                        self.logger.warning(f"Failed to extract index {index}: {e}")
                        return default

                # Attempt to locate and parse additional metrics
                co_authors_text = driver.find_element(By.CSS_SELECTOR, '#co-authors > span').text if driver.find_elements(By.CSS_SELECTOR, '#co-authors > span') else ''
                cited_by_text = driver.find_element(By.CSS_SELECTOR, '#cited-by > span').text if driver.find_elements(By.CSS_SELECTOR, '#cited-by > span') else ''
                preprints_text = driver.find_element(By.CSS_SELECTOR, '#preprints > span').text if driver.find_elements(By.CSS_SELECTOR, '#preprints > span') else ''

                return {
                    'author_id': author_id,
                    'citations': safe_extract(data, 0),
                    'documents': safe_extract(data, 2),
                    'h_index': safe_extract(data, 4),
                    'co_authors_count': self.str2int(co_authors_text.split(' ')[0]) if co_authors_text and ' ' in co_authors_text else 0,
                    'cited_by_count': self.str2int(cited_by_text.split(' ')[2]) if cited_by_text and len(cited_by_text.split(' ')) > 2 else 0,
                    'preprints_count': self.str2int(preprints_text.split(' ')[0]) if preprints_text and ' ' in preprints_text else 0
                }

            except Exception as parse_error:
                self.logger.error(f"Parsing error for auid {author_id}: {parse_error}")
                self._log_failed_auid(author)
                return None

        except Exception as e:
            self.logger.error(f"Comprehensive scraping error for auid {author_id}: {e}")
            self._log_failed_auid(author)
            return None

        finally:
            if driver:
                try:
                    driver.quit()
                except Exception as quit_error:
                    self.logger.warning(f"Error closing driver: {quit_error}")

    def process_file(self, auid_chunk):
        """
        Process a single file with comprehensive error handling.
        """
        try:
            if not isinstance(auid_chunk, list):
                authors = [auid_chunk]

            authors = auid_chunk

            # Scrape data for each author using ThreadPoolExecutor
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                author_results = list(
                    executor.map(self.scrape_author_data, authors)
                )

            return author_results

        except Exception as e:
            self.logger.error(f"Error processing of auids chuck {len(authors)} auid: {e}")
            return None

    def run(self):
        try:
            with open(self.input_dir) as file:
                data = json.load(file)

            chunk_size = 100
            # 20241206_215254, 21000
            chunks = [data[i:i + chunk_size] for i in range(7900, len(data), chunk_size)]

            with tqdm(chunks, desc="Processing Chunks") as pbar:
                for chunk in pbar:
                    result = self.process_file(chunk)
                    if result:
                        transformed_data = [{
                            "auid": author['author_id'],
                            "author_stat": {
                                "citations": author['citations'],
                                "documents": author['documents'],
                                "h_index": author['h_index'],
                                "co_authors_count": author['co_authors_count'],
                                "cited_by_count": author['cited_by_count'],
                                "preprints_count": author['preprints_count']
                            }
                        } for author in result if author is not None]

                        # Save results with timestamp
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        output_file = os.path.join(self.output_dir, f'author_metrics_{timestamp}.json')
                        with open(output_file, 'w') as f:
                            json.dump(transformed_data, f, indent=2)
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        self.logger.info(f"Processed Chunk of {len(result)} auids successfully")
                        self.logger.info(f"Results saved to {output_file}")
                        
                    pbar.set_postfix({"processed": len(result)})

            # Save results with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.logger.info(f"Processed {len(data)} files successfully")

            # Write remaining failed auids at end of run
            if self.failed_auids:
                with open(self.failed_auids_file, 'w') as f:
                    json.dump(self.failed_auids, f, indent=2)
                self.logger.warning(f"Final log of {len(self.failed_auids)} failed AUIDs")

        finally:
            # Ensure writing happens even if exception occurs
            if hasattr(self, 'failed_auids') and self.failed_auids:
                with open(self.failed_auids_file, 'w') as f:
                    json.dump(self.failed_auids, f, indent=2)



def main():
    """Diagnostic entry point for the script."""
    # Diagnostic print of system information
    print("System Diagnostics:")
    print(f"Python Version: {sys.version}")
    print(f"Operating System: {sys.platform}")
    print(f"Current Working Directory: {os.getcwd()}")

    try:
        scraper = AuthorScraper(
            input_dir=input_file,  # Adjust to your Colab file path
            output_dir="results",
            log_dir="logs",
            max_workers=4,
            rate_limit_delay=(1, 3),
            retry_max_attempts=3
        )
        scraper.run()
    except Exception as e:
        print(f"Critical Error in Main Execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()