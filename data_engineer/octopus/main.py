from src.spark.streaming import ScopusProcessor
from src.spark.scrape_streaming import ScopusScraperProcessor
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
    # Process Scrape data
    processor = ScopusScraperProcessor()
    processor.run()