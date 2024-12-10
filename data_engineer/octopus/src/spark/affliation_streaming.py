import os
import orjson as json
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col, when, lit
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class AffiliationProcessor:
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
        
        self.spark = SparkSession.builder.appName("AffiliationAnalysis")
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
            CREATE TABLE IF NOT EXISTS scopus_data.affiliations (
                affiliation_id text PRIMARY KEY,
                preferred_name text,
                name_variants list<text>,
                documents_count int,
                authors_count int,
                country text,
                city text,
                state text,
                street_address text,
                postal_code text,
                contact_url text,
                hierarchy_ids list<text>
            )
        """)

        session.shutdown()

        self.schema = StructType([
            StructField("@affid", StringType(), True),
            StructField("preferredName", StringType(), True),
            StructField("nameVariants", ArrayType(StringType()), True),
            StructField("metrics", StructType([
                StructField("documentsCount", IntegerType(), True),
                StructField("authorsCount", IntegerType(), True)
            ]), True),
            StructField("address", StructType([
                StructField("country", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("streetAddress", StringType(), True),
                StructField("postalCode", StringType(), True)
            ]), True),
            StructField("hierarchyIds", ArrayType(StringType()), True),
            StructField("contact", StringType(), True)
        ])

    def validate_and_transform_data(self, df):
        """Validate and transform DataFrame, handling null values and data quality issues"""
        valid_df = df.filter(col("@affid").isNotNull())
        
        if valid_df.count() < df.count():
            dropped_count = df.count() - valid_df.count()
            logger.warning(f"Dropped {dropped_count} records with null affiliation_id")
        
        transformed_df = valid_df.select(
            col("@affid").alias("affiliation_id"),
            when(col("preferredName").isNotNull(), col("preferredName"))
                .otherwise("Unknown").alias("preferred_name"),
            when(col("nameVariants").isNotNull(), col("nameVariants"))
                .otherwise(lit([])).alias("name_variants"),
            when(col("metrics.documentsCount").isNotNull(), col("metrics.documentsCount"))
                .otherwise(0).alias("documents_count"),
            when(col("metrics.authorsCount").isNotNull(), col("metrics.authorsCount"))
                .otherwise(0).alias("authors_count"),
            when(col("address.country").isNotNull(), col("address.country"))
                .otherwise("Unknown").alias("country"),
            when(col("address.city").isNotNull(), col("address.city"))
                .otherwise("Unknown").alias("city"),
            when(col("address.state").isNotNull(), col("address.state"))
                .otherwise("Unknown").alias("state"),
            when(col("address.streetAddress").isNotNull(), col("address.streetAddress"))
                .otherwise("Unknown").alias("street_address"),
            when(col("address.postalCode").isNotNull(), col("address.postalCode"))
                .otherwise("Unknown").alias("postal_code"),
            when(col("contact").isNotNull(), col("contact"))
                .otherwise("Unknown").alias("contact_url"),
            when(col("hierarchyIds").isNotNull(), col("hierarchyIds"))
                .otherwise(lit([])).alias("hierarchy_ids")
        )
        
        return transformed_df

    def process_file(self, file_path):
        """Process a JSON file containing an array of affiliation records"""
        try:
            logger.info(f"Reading affiliations from file: {file_path}")
            
            # Read the JSON array from file
            with open(file_path, 'rb') as f:
                # Remove trailing comma if present and wrap in array if needed
                content = f.read()
                if content.strip().endswith(b','):
                    content = content.strip()[:-1]
                if not content.strip().startswith(b'['):
                    content = b'[' + content + b']'
                
                # Parse the JSON array
                affiliations_array = json.loads(content)
            
            # Convert array to JSON strings for Spark processing
            json_strings = [json.dumps(record).decode() for record in affiliations_array]
            
            # Create DataFrame from JSON strings
            rdd = self.spark.sparkContext.parallelize(json_strings)
            df = self.spark.read.schema(self.schema).json(rdd)
            
            # Validate and transform data
            transformed_df = self.validate_and_transform_data(df)
            
            # Check if we have any valid records
            record_count = transformed_df.count()
            if record_count == 0:
                logger.warning("No valid records found in file")
                return
            
            # Write to Cassandra
            transformed_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="affiliations", keyspace="scopus_data") \
                .mode("append") \
                .save()

            transformed_df.unpersist()
            logger.info(f"Successfully processed {record_count} affiliations from file")

        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            raise

    def close(self):
        """Clean up resources"""
        if self.spark:
            self.spark.stop()