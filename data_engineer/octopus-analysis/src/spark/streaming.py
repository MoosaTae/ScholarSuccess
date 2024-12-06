import os

import orjson as json
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from src.ingestion.scopus_loader import StreamingScopusLoader
from src.spark.transform import ScopusTransformer
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class ScopusProcessor:
    def __init__(self):
        
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
        self.spark = SparkSession.builder.appName("ScopusAnalysis")

        for key, value in configs.items():
            self.spark = self.spark.config(key, value)
        self.spark = self.spark.getOrCreate()
            
        cluster = Cluster([db_host])
        session = cluster.connect()
        self.transformer = ScopusTransformer()
        self.chunk_size = 1000

        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS scopus_data
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
        """)

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

        self.loader = StreamingScopusLoader(chunk_size=self.chunk_size)
        self.loader.rename_files_to_json()


    def process_chunk(self, chunk):
        try:
            # Convert each record to JSON string
            json_strings = [json.dumps(record).decode() for record in chunk]
            # Create an RDD from the JSON strings
            rdd = self.spark.sparkContext.parallelize(json_strings)
            # Read JSON data into DataFrame with inferred schema
            df = self.spark.read.json(rdd)
    
            # Transform data
            transformed_df = self.transformer.apply_all_transforms(df)
    
            # Write to Cassandra
            transformed_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="records", keyspace="scopus_data") \
                .mode("append") \
                .save()

            transformed_df.unpersist()
            
            logger.info("Successfully wrote chunk to Cassandra")
    
        except Exception as e:
            logger.error(f"Error processing chunk: {str(e)}")# Log first record in the chunk for debugging
            raise
        
        
    def run(self):
        try:
            logger.info("Starting data processing pipeline...")

            for chunk in self.loader.process_data():
                self.process_chunk(chunk)

            logger.info("Pipeline completed successfully")

        except Exception as e:
            logger.error(f"Pipeline error: {str(e)}")
            raise

        finally:
            self.spark.stop()