import orjson as json
from typing import Dict, List, Generator, Iterator
from pathlib import Path
from src.utils.logger import setup_logger
import os

logger = setup_logger(__name__)

class ScopusScraperLoader:
    def __init__(self, base_path: str = None, chunk_size: int = 1000):
        """
        Initialize the scraper loader with configurable chunk size.
        
        Args:
            base_path: Path to the scraped scopus data directory
            chunk_size: Number of records to yield in each batch
        """
        self.base_path = Path(base_path or os.getenv("SCOPUS_SCRAPE_PATH", "./scrape/"))
        self.chunk_size = chunk_size
        self.year_range = range(2013, 2024)  
        
    def validate_directory_structure(self):
        """
        Validate that all expected year directories exist and contain JSON files.
        """
        missing_dirs = []
        empty_dirs = []
        
        for year in self.year_range:
            year_dir = self.base_path / str(year)
            if not year_dir.exists():
                missing_dirs.append(year)
            elif not any(year_dir.glob('*.json')):
                empty_dirs.append(year)
                
        if missing_dirs:
            logger.warning(f"Missing year directories: {missing_dirs}")
        if empty_dirs:
            logger.warning(f"Year directories with no JSON files: {empty_dirs}")
            
        return len(missing_dirs) == 0 and len(empty_dirs) == 0
    
    def stream_files(self) -> Generator[Path, None, None]:
        """
        Yield file paths one at a time, organized by year.
        """
        for year in self.year_range:
            year_dir = self.base_path / str(year)
            if year_dir.is_dir():
                logger.info(f"Processing year directory: {year}")
                for file_path in year_dir.glob('*.json'):
                    if file_path.is_file() and not file_path.name.startswith('.'):
                        logger.debug(f"Found file: {file_path}")
                        yield file_path
                        
    def stream_records(self, file_path: Path) -> Iterator[Dict]:
        """
        Stream individual records from a JSON file.
        """
        if file_path.stat().st_size == 0:
            logger.warning(f"Skipping empty file: {file_path}")
            return
            
        try:
            with open(file_path, 'r', encoding='utf-8', buffering=1024*1024) as file:
                try:
                    # First try to load the entire file as a JSON array
                    data = json.loads(file.read())
                    if isinstance(data, list):
                        yield from data
                    elif isinstance(data, dict):
                        yield data
                    else:
                        logger.error(f"Unexpected JSON structure in {file_path}")
                except json.JSONDecodeError:
                    # If that fails, try streaming record by record
                    file.seek(0)
                    buffer = ''
                    for line in file:
                        buffer += line
                        try:
                            record = json.loads(buffer)
                            yield record
                            buffer = ''
                        except json.JSONDecodeError:
                            continue
                            
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            raise
            
    def process_data(self) -> Generator[List[Dict], None, None]:
        """
        Process the scraped data and yield in chunks of size `chunk_size`.
        Validates directory structure before processing.
        """
        if not self.validate_directory_structure():
            logger.warning("Directory structure validation failed, but continuing processing")
            
        current_chunk = []
        processed_count = 0
        
        for file_path in self.stream_files():
            try:
                for record in self.stream_records(file_path):
                    current_chunk.append(record)
                    processed_count += 1
                    
                    if len(current_chunk) >= self.chunk_size:
                        logger.info(f"Yielding chunk of size: {len(current_chunk)} | Total processed: {processed_count}")
                        yield current_chunk
                        current_chunk = []
                        
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {str(e)}")
                continue
                
        if current_chunk:
            logger.info(f"Yielding final chunk of size: {len(current_chunk)} | Total processed: {processed_count}")
            yield current_chunk