from typing import Dict, Any
import os
from pathlib import Path

class EnvironmentConfig:
    """
    Base configuration class that will be inherited by environment-specific configs.
    This helps us maintain different settings for development, testing, and production.
    """
    # Kafka settings
    KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
    KAFKA_TOPIC = 'scopus_data'
    
    # Redis settings
    REDIS_HOST = 'redis'
    REDIS_PORT = 6379
    
    # Cassandra settings
    CASSANDRA_HOSTS = ['cassandra']
    CASSANDRA_KEYSPACE = 'scopus_analysis'
    
    # Spark settings
    SPARK_MASTER = 'spark://spark:7077'
    
    # Data paths - using Path for cross-platform compatibility
    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_DIR = PROJECT_ROOT / 'data'
    SCOPUS_DATA_PATH = DATA_DIR / 'scopus'
    SCRAPE_OUTPUT_PATH = DATA_DIR / 'scraped'
    
    # Postgres settings for Airflow
    POSTGRES_HOST = 'postgres'
    POSTGRES_USER = 'airflow'
    POSTGRES_PASSWORD = 'airflow'
    POSTGRES_DB = 'airflow'
    
    # Monitoring settings
    PROMETHEUS_PORT = 9090
    GRAFANA_PORT = 3000
    
    @classmethod
    def as_dict(cls) -> Dict[str, Any]:
        """
        Convert config to dictionary, including only uppercase attributes
        which is a common pattern for configuration variables
        """
        return {
            key: getattr(cls, key)
            for key in dir(cls)
            if key.isupper()
        }
    
    @classmethod
    def validate_paths(cls) -> None:
        """
        Ensure all required data directories exist.
        Creates them if they don't exist.
        """
        cls.DATA_DIR.mkdir(exist_ok=True)
        cls.SCOPUS_DATA_PATH.mkdir(exist_ok=True)
        cls.SCRAPE_OUTPUT_PATH.mkdir(exist_ok=True)

class DevelopmentConfig(EnvironmentConfig):
    """Development environment specific configuration"""
    DEBUG = True
    ENVIRONMENT = 'development'
    
class ProductionConfig(EnvironmentConfig):
    """Production environment specific configuration"""
    DEBUG = False
    ENVIRONMENT = 'production'
    # In production, we might want to use different hosts or additional security settings
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_SERVERS', 'kafka:9092')
    CASSANDRA_HOSTS = os.getenv('CASSANDRA_HOSTS', 'cassandra').split(',')

class TestingConfig(EnvironmentConfig):
    """Testing environment specific configuration"""
    DEBUG = True
    ENVIRONMENT = 'testing'
    # Use in-memory or test-specific databases
    CASSANDRA_KEYSPACE = 'scopus_test'
    
# Choose configuration based on environment variable
CONFIG_MAP = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig
}

# Get the active configuration
def get_config():
    """
    Returns the appropriate configuration based on the environment.
    Defaults to development if not specified.
    """
    env = os.getenv('ENVIRONMENT', 'development')
    config_class = CONFIG_MAP.get(env, DevelopmentConfig)
    # Ensure data directories exist
    config_class.validate_paths()
    return config_class

# Create a configuration instance for easy import
# Usage: `From config import config`
# Then access configuration variables like `config.KAFKA_BOOTSTRAP_SERVERS`
Config = get_config()