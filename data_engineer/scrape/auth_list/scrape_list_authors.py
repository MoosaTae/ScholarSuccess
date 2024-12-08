import os
import json
import time
import random
import logging
from typing import List, Dict, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import tqdm

class AbstractFetcher:
    """
    A comprehensive tool for fetching research paper abstracts using Scopus API.
    
    Manages rate limiting, concurrent processing, and error handling for abstract retrieval.
    """

    def __init__(
        self, 
        api_key: str, 
        data: List[Dict], 
        max_workers: int = 3, 
        max_retries: int = 3
    ):
        """
        Initialize the AbstractFetcher.
        
        Args:
            api_key (str): Scopus API key
            data (List[Dict]): List of papers to process
            max_workers (int): Number of concurrent workers
            max_retries (int): Maximum retry attempts for each request
        """
        self._setup_logging()
        
        self.api_key = api_key
        self.data = data
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.session = requests.Session()
    
    def _setup_logging(self) -> None:
        """Configure logging with console handler only."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s: %(message)s',
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger(__name__)
    
    def _make_api_request(self, url: str, doi: str) -> Optional[Dict]:
        """
        Make a rate-limited API request with exponential backoff.
        
        Args:
            url (str): API endpoint URL
            doi (str): Document object identifier
        
        Returns:
            Optional dictionary of API response
        """
        headers = {
            'X-ELS-APIKey': self.api_key,
            'Accept': 'application/json'
        }

        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, headers=headers, timeout=10)
                
                if response.status_code == 429:  # Rate limit error
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
                
                if attempt == self.max_retries - 1:
                    return None
                
                time.sleep(1)
        
        return None
    
    def _parse_abstract_data(self, data: Dict, doi: str) -> Optional[Dict]:
        """
        Parse and extract relevant data from API response.
        
        Args:
            data (Dict): API response data
            doi (str): Document object identifier
        
        Returns:
            Dictionary of parsed abstract data
        """
        try:
            response = data.get('abstracts-retrieval-response', {}).get('coredata', {})
            
            # Extract creator with robust handling
            creator = response.get('dc:creator')
            if isinstance(creator, dict):
                creator = creator.get('author')
            elif isinstance(creator, list):
                creator = [a.get('author', a) for a in creator]
            
            return {
                'doi': doi,
                'author': creator,
                'source_id': response.get('source-id')
            }
        
        except Exception as parse_error:
            self.logger.error(f"Data parsing error for DOI {doi}: {parse_error}")
            return None
    
    def _process_paper(self, paper: Dict) -> Optional[Dict]:
        """
        Process a single paper by fetching and parsing its abstract.
        
        Args:
            paper (Dict): Paper metadata dictionary
        
        Returns:
            Processed paper data or None
        """
        try:
            doi = paper.get('prism:doi')
            url = paper.get('prism:url')
            
            if not doi or not url:
                self.logger.warning(f"Missing DOI or URL for paper: {paper}")
                return None
            
            # Fetch API data
            data = self._make_api_request(url, doi)
            
            if data is None:
                self.logger.warning(f"No data retrieved for DOI: {doi}")
                return None
            
            # Parse and return abstract data
            return self._parse_abstract_data(data, doi)
        
        except Exception as e:
            self.logger.error(f"Processing error for paper: {e}")
            return None
    
    def fetch_abstracts(self) -> List[Dict]:
        """
        Fetch abstracts for all papers concurrently.
        
        Returns:
            List of processed paper data
        """
        self.logger.info(f"Processing {len(self.data)} papers")
        processed_data = self.data.copy()
        
        successful_retrievals = 0
        failed_retrievals = 0
        
        # Use ThreadPoolExecutor for concurrent processing
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            progress_bar = tqdm.tqdm(
                total=len(self.data), 
                desc="Fetching Abstracts", 
                unit="paper"
            )
            
            futures = {
                executor.submit(self._process_paper, paper): paper 
                for paper in self.data
            }
            
            for future in as_completed(futures):
                result = future.result()
                progress_bar.update(1)
                
                if result:
                    for updated_paper in processed_data:
                        if updated_paper['prism:doi'] == result['doi']:
                            if result['author'] is not None:
                                updated_paper['author'] = result['author']
                            if result['source_id'] is not None:
                                updated_paper['source-id'] = result['source_id']
                            successful_retrievals += 1
                            break
                else:
                    failed_retrievals += 1
            
            progress_bar.close()
        
        # Log retrieval statistics
        self._log_retrieval_stats(
            total_papers=len(self.data),
            successful=successful_retrievals,
            failed=failed_retrievals
        )
        
        return processed_data
    
    def _log_retrieval_stats(self, total_papers: int, successful: int, failed: int) -> None:
        """
        Log detailed retrieval statistics.
        
        Args:
            total_papers (int): Total number of papers processed
            successful (int): Number of successful retrievals
            failed (int): Number of failed retrievals
        """
        self.logger.info("Retrieval Statistics:")
        self.logger.info(f"Total Papers: {total_papers}")
        self.logger.info(f"Successful Retrievals: {successful}")
        self.logger.info(f"Failed Retrievals: {failed}")
        self.logger.info(f"Success Rate: {successful / total_papers * 100:.2f}%")

def main():
    """
    Main entry point for the abstract fetching process.
    """
    try:
        # Example input data
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
        
        # Replace with your actual Scopus API key
        api_key = os.environ.get('SCOPUS_API_KEY', 'your_api_key_here')
        
        start_time = time.time()
        
        # Initialize and run the fetcher
        fetcher = AbstractFetcher(
            api_key=api_key, 
            data=example_data, 
            max_workers=3, 
            max_retries=3
        )
        updated_data = fetcher.fetch_abstracts()
        
        end_time = time.time()
        print(f"Total processing time: {end_time - start_time:.2f} seconds")
        
        # Pretty print the results
        print(json.dumps(updated_data, indent=2))
    
    except Exception as e:
        logging.error(f"Critical error in main execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()