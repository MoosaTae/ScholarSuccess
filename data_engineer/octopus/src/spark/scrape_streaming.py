import os
import glob
from pathlib import Path
import orjson as json
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType
from pyspark.sql.functions import col, when, lit, coalesce
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class ScrapeProcessor:
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
        
        self.spark = SparkSession.builder.appName("ScrapeAnalysis")
        for key, value in configs.items():
            self.spark = self.spark.config(key, value)
        self.spark = self.spark.getOrCreate()

        # Create Cassandra keyspace and table (using same schema as ScopusProcessor)
        cluster = Cluster([db_host])
        session = cluster.connect()
        
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

        # Define schema for scrape data
        self.scrape_schema = StructType([
            StructField("prism:doi", StringType(), True),
            StructField("eid", StringType(), True),
            StructField("dc:title", StringType(), True),
            StructField("dc:creator", StringType(), True),
            StructField("prism:publicationName", StringType(), True),
            StructField("prism:coverDate", StringType(), True),
            StructField("citedby-count", StringType(), True),
            StructField("openaccessFlag", BooleanType(), True),
            StructField("abstract", StringType(), True),
            StructField("subtypeDescription", StringType(), True),
            StructField("prism:aggregationType", StringType(), True),
            StructField("source-id", StringType(), True),
            StructField("prism:issn", StringType(), True),
            StructField("author", ArrayType(StructType([
                StructField("ce:given-name", StringType(), True),
                StructField("ce:surname", StringType(), True),
                StructField("author-url", StringType(), True)
            ])), True),
            StructField("affiliation", ArrayType(StructType([
                StructField("affilname", StringType(), True)
            ])), True)
        ])

        # Define schema for reference count data
        self.refcount_schema = StructType([
            StructField("EID", StringType(), True),
            StructField("DOI", StringType(), True),
            StructField("References", IntegerType(), True)
        ])

    def read_json_files(self, folder_path, schema):
        """Read all JSON files in a folder and return a DataFrame"""
        all_records = []
        
        json_files = glob.glob(os.path.join(folder_path, "*.json"))
        for file_path in json_files:
            try:
                with open(file_path, 'rb') as f:
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
            return None
            
        json_strings = [json.dumps(record).decode() for record in all_records]
        rdd = self.spark.sparkContext.parallelize(json_strings)
        return self.spark.read.schema(schema).json(rdd)

    def process_year(self, year, scrape_base_path, refcount_base_path):
        """Process data for a specific year"""
        try:
            # Read scrape data
            scrape_folder = os.path.join(scrape_base_path, str(year))
            scrape_df = self.read_json_files(scrape_folder, self.scrape_schema)
            
            if scrape_df is None:
                logger.warning(f"No scrape data found for year {year}")
                return
            
            # Read reference count data
            refcount_file = os.path.join(refcount_base_path, f"{year}.json")
            refcount_df = self.read_json_files(os.path.dirname(refcount_file), self.refcount_schema)
            
            if refcount_df is None:
                logger.warning(f"No reference count data found for year {year}")
                refcount_df = self.spark.createDataFrame([], self.refcount_schema)
            
            # Join dataframes on DOI
            joined_df = scrape_df.join(
                refcount_df.select(
                    col("DOI").alias("ref_doi"),
                    col("References").alias("ref_count")
                ),
                col("prism:doi") == col("ref_doi"),
                "left_outer"
            )
            
            # Transform to match the records table schema
            transformed_df = joined_df.select(
                col("prism:doi").alias("doi"),
                col("dc:title").alias("title"),
                col("abstract"),
                col("subtypeDescription").alias("document_type"),
                col("prism:aggregationType").alias("source_type"),
                col("prism:coverDate").alias("publication_date"),
                col("prism:publicationName").alias("source_title"),
                lit(None).cast(StringType()).alias("publisher"),
                lit(None).cast(StringType()).alias("author_keywords"),
                lit(None).cast(StringType()).alias("subject_code"),
                lit(None).cast(StringType()).alias("subject_name"),
                lit(None).cast(StringType()).alias("subject_abbrev"),
                when(col("author").isNotNull(), 
                     col("author").getItem(0).getField("ce:given-name"))
                    .otherwise(None).alias("author_given_name"),
                when(col("author").isNotNull(),
                     col("author").getItem(0).getField("ce:surname"))
                    .otherwise(None).alias("author_surname"),
                when(col("author").isNotNull(),
                     col("author").getItem(0).getField("author-url"))
                    .otherwise(None).alias("author_url"),
                when(col("affiliation").isNotNull(),
                     col("affiliation").getItem(0).getField("affilname"))
                    .otherwise(None).alias("author_affiliation"),
                lit(None).cast(StringType()).alias("author_details"),
                lit(None).cast(StringType()).alias("funding_details"),
                coalesce(col("ref_count"), lit(0)).alias("ref_count"),
                col("openaccessFlag").alias("open_access"),
                when(col("affiliation").isNotNull(),
                     col("affiliation").getItem(0).getField("affilname"))
                    .otherwise(None).alias("affiliation"),
                lit(None).cast(StringType()).alias("language"),
                coalesce(col("citedby-count").cast("int"), lit(0)).alias("cited_by"),
                lit(None).cast(StringType()).alias("status_state"),
                lit(None).cast(StringType()).alias("delivered_date"),
                lit(None).cast(StringType()).alias("subject_area")
            )
            
            # Write to Cassandra
            transformed_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="records", keyspace="scopus_data") \
                .mode("append") \
                .save()

            transformed_df.unpersist()
            logger.info(f"Successfully processed year {year}")

        except Exception as e:
            logger.error(f"Error processing year {year}: {str(e)}")
            raise

    def process_all_years(self, scrape_base_path, refcount_base_path, start_year=2013, end_year=2023):
        """Process all years in the given range"""
        try:
            for year in range(start_year, end_year + 1):
                logger.info(f"Processing year {year}")
                self.process_year(year, scrape_base_path, refcount_base_path)
                
            logger.info("Completed processing all years")
            
        except Exception as e:
            logger.error(f"Error processing years: {str(e)}")
            raise

    def close(self):
        """Clean up resources"""
        if self.spark:
            self.spark.stop()