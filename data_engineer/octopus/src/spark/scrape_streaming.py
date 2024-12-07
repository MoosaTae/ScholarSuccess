import os
import orjson as json
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from src.ingestion.scrape_loader import ScopusScraperLoader
from src.spark.scrape_transformer import ScopusScraperTransformer
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class ScopusScraperProcessor:
    def __init__(self):
        # Initialize Spark session with configurations
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = os.getenv("DB_PORT", "9042")
        configs = {
            "spark.jars.packages": "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0",
            "spark.cassandra.connection.host": db_host,
            "spark.cassandra.connection.port": db_port,
            "spark.driver.memory": "8g",
            "spark.executor.memory": "8g",
            "spark.sql.shuffle.partitions": "100",
            "spark.default.parallelism": "100",
        }
        
        self.spark = SparkSession.builder.appName("ScopusScraperAnalysis")

        for key, value in configs.items():
            self.spark = self.spark.config(key, value)
        self.spark = self.spark.getOrCreate()
            
        # Initialize Cassandra connection and verify keyspace/table
        cluster = Cluster([db_host])
        session = cluster.connect()
        
        # Ensure keyspace exists (reusing same keyspace as original processor)
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS scopus_data
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
        """)

        # Verify table exists (using same table as original processor)
        session.execute("""
            CREATE TABLE IF NOT EXISTS scopus_data.records (
                doi text PRIMARY KEY,
                title text,
                abstract text,
                document_type text,
                source_type text,
                publication_date text,
                source_title text,
                publisher text,
                author_keywords text,
                subject_code text,
                subject_name text,
                subject_abbrev text,
                author_given_name text,
                author_surname text,
                author_url text,
                author_affiliation text,
                author_details text,
                funding_details text,
                ref_count int,
                open_access boolean,
                affiliation text,
                language text,
                cited_by int,
                status_state text,
                delivered_date text,
                subject_area text
            )
        """)

        session.shutdown()

        # Initialize components
        self.transformer = ScopusScraperTransformer()
        self.chunk_size = 1000
        self.loader = ScopusScraperLoader(chunk_size=self.chunk_size)

    def process_chunk(self, chunk):
        """Process a chunk of scraped Scopus records"""
        try:
            # Convert records to JSON strings
            json_strings = [json.dumps(record).decode() for record in chunk]
            
            # Create RDD from JSON strings
            rdd = self.spark.sparkContext.parallelize(json_strings)
            
            # Convert to DataFrame
            df = self.spark.read.json(rdd)
            
            # Apply transformations
            transformed_df = self.transformer.apply_all_transforms(df)
            
            # Write to Cassandra (same table as original data)
            transformed_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="records", keyspace="scopus_data") \
                .mode("append") \
                .save()

            # Clean up
            transformed_df.unpersist()
            
            logger.info(f"Successfully processed and wrote chunk of {len(chunk)} records to Cassandra")
            
        except Exception as e:
            logger.error(f"Error processing chunk: {str(e)}")
            # Log the first record for debugging - using str() instead of json.dumps with indent
            if chunk:
                logger.error(f"First record in failed chunk: {str(chunk[0])}")
            raise

    def run(self):
        """Run the complete processing pipeline"""
        try:
            logger.info("Starting scraped data processing pipeline...")
            
            # Validate directory structure before processing
            if not self.loader.validate_directory_structure():
                logger.warning("Some year directories are missing or empty")
            
            # Process data in chunks
            for chunk in self.loader.process_data():
                self.process_chunk(chunk)
                
            logger.info("Scraper pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline error: {str(e)}")
            raise
            
        finally:
            self.spark.stop()

    def run_single_year(self, year: int):
        """Run the pipeline for a specific year"""
        try:
            logger.info(f"Starting scraped data processing for year {year}...")
            
            # Process data in chunks for specific year
            for chunk in self.loader.process_data_for_year(year):
                self.process_chunk(chunk)
                
            logger.info(f"Successfully completed processing for year {year}")
            
        except Exception as e:
            logger.error(f"Error processing year {year}: {str(e)}")
            raise
            
        finally:
            self.spark.stop()