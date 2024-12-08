import os
import sys
import json
import time
import random
import logging
import concurrent.futures
from typing import List, Dict, Optional, Union
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

class AuthorScraper:
    """
    A web scraper for retrieving author metrics from Scopus.
    
    Attributes:
        data (List[Dict]): List of authors to scrape
        max_workers (int): Maximum number of concurrent workers
        rate_limit_delay (tuple): Range of random delay between requests
        retry_max_attempts (int): Maximum retry attempts for scraping
    """

    def __init__(
        self, 
        data: List[Dict], 
        max_workers: int = 3, 
        rate_limit_delay: tuple = (1, 3), 
        retry_max_attempts: int = 3
    ):
        self.data = data
        self.max_workers = max_workers
        self.rate_limit_delay = rate_limit_delay
        self.retry_max_attempts = retry_max_attempts
        self._setup_logging()

    def _setup_logging(self) -> None:
        """Configure logging settings."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s: %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        self.logger = logging.getLogger(__name__)

    def _log_failed_auid(self, author: Union[Dict, str]) -> None:
        """
        Log failed author ID scraping attempts.
        
        Args:
            author: Author identifier or dictionary
        """
        try:
            auid = author.get('@auid', str(author)) if isinstance(author, dict) else str(author)
            self.logger.warning(f"Failed to scrape data for AUID: {auid}")
        except Exception as log_error:
            self.logger.error(f"Error logging failed AUID: {log_error}")

    def _create_webdriver(self) -> webdriver.Chrome:
        """
        Create and configure a Chrome WebDriver.
        
        Returns:
            Configured Chrome WebDriver
        """
        chrome_options = Options()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--log-level=3')

        service = Service(ChromeDriverManager().install())
        
        try:
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.implicitly_wait(10)
            return driver
        except Exception as e:
            self.logger.error(f"WebDriver initialization error: {e}")
            raise

    @staticmethod
    def _parse_number(value: str) -> int:
        """
        Convert string to integer, handling various formats.
        
        Args:
            value: String representation of a number
        
        Returns:
            Parsed integer value
        """
        try:
            return int(str(value).replace(',', ''))
        except (ValueError, AttributeError, TypeError):
            return 0

    def _extract_metrics(self, driver: webdriver.Chrome, author_id: str) -> Optional[Dict]:
        """
        Extract author metrics from Scopus page.
        
        Args:
            driver: Active WebDriver
            author_id: Scopus author identifier
        
        Returns:
            Dictionary of author metrics or None if extraction fails
        """
        try:
            # Wait for metrics section to load
            metrics_section = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".MetricSection-module__s8lWB"))
            )
            metrics_text = metrics_section.text.split("\n")

            # Extract metrics with safe parsing
            metrics = {
                'author_id': author_id,
                'citations': self._parse_number(metrics_text[0]) if len(metrics_text) > 0 else 0,
                'documents': self._parse_number(metrics_text[2]) if len(metrics_text) > 2 else 0,
                'h_index': self._parse_number(metrics_text[4]) if len(metrics_text) > 4 else 0,
                'co_authors_count': self._extract_count(driver, '#co-authors'),
                'cited_by_count': self._extract_count(driver, '#cited-by', index=2),
                'preprints_count': self._extract_count(driver, '#preprints')
            }

            return metrics
        except Exception as e:
            self.logger.error(f"Metrics extraction error for AUID {author_id}: {e}")
            return None

    def _extract_count(self, driver: webdriver.Chrome, selector: str, index: int = 0) -> int:
        """
        Extract count from a specific element.
        
        Args:
            driver: Active WebDriver
            selector: CSS selector
            index: Index of text to parse
        
        Returns:
            Extracted count as integer
        """
        try:
            element = driver.find_element(By.CSS_SELECTOR, f"{selector} > span")
            text_parts = element.text.split()
            return self._parse_number(text_parts[index]) if text_parts else 0
        except Exception:
            return 0

    def scrape_author_data(self, author: Dict) -> Optional[Dict]:
        """
        Scrape data for a single author.
        
        Args:
            author: Author dictionary with '@auid' key
        
        Returns:
            Scraped author metrics or None
        """
        if not isinstance(author, dict):
            self.logger.warning(f"Invalid author data type: {type(author)}")
            self._log_failed_auid(author)
            return None

        author_id = author.get('@auid')
        if not author_id:
            self.logger.warning("Skipping author with invalid ID")
            return None

        driver = None
        try:
            time.sleep(random.uniform(*self.rate_limit_delay))
            driver = self._create_webdriver()
            
            url = f'https://www.scopus.com/authid/detail.uri?authorId={author_id}'
            driver.get(url)

            return self._extract_metrics(driver, author_id)
        except Exception as e:
            self.logger.error(f"Scraping error for AUID {author_id}: {e}")
            self._log_failed_auid(author)
            return None
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception as quit_error:
                    self.logger.warning(f"Error closing driver: {quit_error}")

    def process_authors(self, authors: List[Dict]) -> List[Dict]:
        """
        Process a list of authors using concurrent processing.
        
        Args:
            authors: List of author dictionaries
        
        Returns:
            List of processed author metrics
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            author_results = list(filter(None, executor.map(self.scrape_author_data, authors)))
        
        return [{
            "auid": author['author_id'],
            "author_stat": {k: v for k, v in author.items() if k != 'author_id'}
        } for author in author_results]

    def run(self) -> List[Dict]:
        """
        Main method to run the scraper.
        
        Returns:
            List of processed author metrics
        """
        chunk_size = 100
        results = []

        for i in range(0, len(self.data), chunk_size):
            chunk = self.data[i:i + chunk_size]
            chunk_results = self.process_authors(chunk)
            results.extend(chunk_results)
            self.logger.info(f"Processed chunk of {len(chunk_results)} authors")

        self.logger.info(f"Total authors processed: {len(results)}")
        return results

def main():
    """Entry point for the script."""
    try:
        example_data = [
            {"@auid": "57113534800"},
            {"@auid": "57217590099"}
        ]

        scraper = AuthorScraper(
            data=example_data,
            max_workers=10,
            rate_limit_delay=(1, 3),
            retry_max_attempts=3
        )
        
        results = scraper.run()
        print(json.dumps(results, indent=2))
    
    except Exception as e:
        logging.error(f"Critical Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()