from src.spark.streaming import ScopusProcessor
from src.spark.scrape_streaming import ScrapeProcessor
from src.spark.affliation_streaming import AffiliationProcessor
from src.spark.author_streaming import AuthorProcessor
import os
from dotenv import load_dotenv

# Load .env file 
load_dotenv()

if __name__ == "__main__":
    # Check if .env are successfully loaded
    db_host = os.getenv("DB_HOST")
    if not db_host:
        raise EnvironmentError("Environment variables not loaded properly!")
    # Process Ajarn data

    processor = ScopusProcessor()
    processor.run()
    # # Process Scrape data
    processor = ScrapeProcessor()
    try:
        processor.process_all_years(
            scrape_base_path="./data/scrape",
            refcount_base_path="./data/refcount",
            start_year=2013,
            end_year=2023
        )
    finally:
        processor.close()
        
        
    # Process Affiliation data
    processor = AffiliationProcessor()
    try:
        processor.process_file("./data/affiliation/all_affid_list2.json")
    finally:
        processor.close()
        
    # Process Author data
    processor = AuthorProcessor()
    try:
        processor.process_all_folders("./data/author")
    finally:
        processor.close()