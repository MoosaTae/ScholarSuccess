import os
import glob
from pathlib import Path
import orjson as json
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import col, when, lit
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class AuthorProcessor:
    def __init__(self):
        db_host = "localhost"
        db_port = os.getenv("DB_PORT", "9042")
        
        configs = {
            "spark.jars.packages": "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0",
            "spark.cassandra.connection.host": db_host,
            "spark.cassandra.connection.port": db_port,
            "spark.driver.memory": "8g",
            "spark.executor.memory": "8g",
        }
        
        self.spark = SparkSession.builder.appName("AuthorMetricsAnalysis")
        for key, value in configs.items():
            self.spark = self.spark.config(key, value)
        self.spark = self.spark.getOrCreate()

        # Create Cassandra keyspace and table
        cluster = Cluster([db_host])
        session = cluster.connect()
        
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS scopus_data
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
        """)

        session.execute("""
            CREATE TABLE IF NOT EXISTS scopus_data.author_metrics (
                author_id text PRIMARY KEY,
                citations bigint,
                documents int,
                h_index int,
                co_authors_count int,
                cited_by_count bigint,
                preprints_count int
            )
        """)

        session.shutdown()

        # Define schema for author metrics data
        self.schema = StructType([
            StructField("auid", StringType(), True),
            StructField("author_stat", StructType([
                StructField("citations", LongType(), True),
                StructField("documents", IntegerType(), True),
                StructField("h_index", IntegerType(), True),
                StructField("co_authors_count", IntegerType(), True),
                StructField("cited_by_count", LongType(), True),
                StructField("preprints_count", IntegerType(), True)
            ]), True)
        ])

    def validate_and_transform_data(self, df):
        """Validate and transform DataFrame, handling null values"""
        valid_df = df.filter(col("auid").isNotNull())
        
        if valid_df.count() < df.count():
            dropped_count = df.count() - valid_df.count()
            logger.warning(f"Dropped {dropped_count} records with null author_id")
        
        transformed_df = valid_df.select(
            col("auid").alias("author_id"),
            when(col("author_stat.citations").isNotNull(), col("author_stat.citations"))
                .otherwise(0).alias("citations"),
            when(col("author_stat.documents").isNotNull(), col("author_stat.documents"))
                .otherwise(0).alias("documents"),
            when(col("author_stat.h_index").isNotNull(), col("author_stat.h_index"))
                .otherwise(0).alias("h_index"),
            when(col("author_stat.co_authors_count").isNotNull(), col("author_stat.co_authors_count"))
                .otherwise(0).alias("co_authors_count"),
            when(col("author_stat.cited_by_count").isNotNull(), col("author_stat.cited_by_count"))
                .otherwise(0).alias("cited_by_count"),
            when(col("author_stat.preprints_count").isNotNull(), col("author_stat.preprints_count"))
                .otherwise(0).alias("preprints_count")
        )
        
        return transformed_df

    def process_folder(self, folder_path):
        """Process all JSON files in a folder"""
        try:
            logger.info(f"Processing folder: {folder_path}")
            
            # Get all JSON files in the folder
            json_files = glob.glob(os.path.join(folder_path, "*.json"))
            
            if not json_files:
                logger.warning(f"No JSON files found in {folder_path}")
                return
            
            all_records = []
            
            # Read and combine all JSON files
            for file_path in json_files:
                try:
                    with open(file_path, 'rb') as f:
                        # Remove trailing comma if present and wrap in array if needed
                        content = f.read()
                        if content.strip().endswith(b','):
                            content = content.strip()[:-1]
                        if not content.strip().startswith(b'['):
                            content = b'[' + content + b']'
                        
                        records = json.loads(content)
                        all_records.extend(records)
                
                except Exception as e:
                    logger.error(f"Error reading file {file_path}: {str(e)}")
                    continue
            
            if not all_records:
                logger.warning("No valid records found in any files")
                return
            
            # Convert to JSON strings for Spark processing
            json_strings = [json.dumps(record).decode() for record in all_records]
            
            # Create DataFrame
            rdd = self.spark.sparkContext.parallelize(json_strings)
            df = self.spark.read.schema(self.schema).json(rdd)
            
            # Validate and transform
            transformed_df = self.validate_and_transform_data(df)
            
            # Write to Cassandra
            transformed_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="author_metrics", keyspace="scopus_data") \
                .mode("append") \
                .save()

            transformed_df.unpersist()
            logger.info(f"Successfully processed {transformed_df.count()} author records from folder")

        except Exception as e:
            logger.error(f"Error processing folder {folder_path}: {str(e)}")
            raise

    def process_all_folders(self, base_path):
        """Process all author_metrics folders"""
        try:
            pattern = os.path.join(base_path, "author_metrics_*")
            folders = glob.glob(pattern)
            
            if not folders:
                logger.error(f"No author_metrics folders found in {base_path}")
                return
            
            for folder in sorted(folders):
                logger.info(f"Processing folder: {folder}")
                self.process_folder(folder)
                
            logger.info("Completed processing all author metrics folders")
            
        except Exception as e:
            logger.error(f"Error processing folders: {str(e)}")
            raise

    def close(self):
        """Clean up resources"""
        if self.spark:
            self.spark.stop()