import json
import os
import logging
import time
from datetime import datetime
from kafka import KafkaConsumer
import snowflake.connector
from snowflake.connector.errors import ProgrammingError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('openweathermap-consumer')

# Kafka configuration
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'openweathermap-weather')
KAFKA_GROUP = os.environ.get('KAFKA_GROUP', 'openweathermap-snowflake-group')

# Snowflake configuration
SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD')
SNOWFLAKE_DATABASE = os.environ.get('SNOWFLAKE_DATABASE', 'ENERGY_DB')
SNOWFLAKE_SCHEMA = os.environ.get('SNOWFLAKE_SCHEMA', 'RAW')
SNOWFLAKE_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE', 'ENERGY_WH')
SNOWFLAKE_ROLE = os.environ.get('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
SNOWFLAKE_TABLE = os.environ.get('SNOWFLAKE_TABLE', 'OPENWEATHERMAP_CURRENT_JSON')

def create_consumer():
    """Create and return a Kafka consumer"""
    retry_count = 0
    max_retries = 10
    
    while retry_count < max_retries:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=KAFKA_GROUP,
                auto_offset_reset='earliest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                enable_auto_commit=False
            )
            logger.info(f"Successfully connected to Kafka broker at {KAFKA_BROKER}")
            return consumer
        except Exception as e:
            retry_count += 1
            logger.warning(f"Failed to connect to Kafka broker (attempt {retry_count}/{max_retries}): {str(e)}")
            time.sleep(5)
    
    raise ConnectionError(f"Failed to connect to Kafka broker after {max_retries} attempts")

def get_snowflake_connection():
    """Create and return a Snowflake connection"""
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE
        )
        logger.info("Successfully connected to Snowflake")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {str(e)}")
        raise

def ensure_table_exists(conn):
    """Ensure the Snowflake table exists"""
    cursor = conn.cursor()
    try:
        # Create database and schema if they don't exist
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")
        
        # Create the raw JSON table
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
            id INTEGER AUTOINCREMENT,
            city VARCHAR,
            received_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            fetch_timestamp TIMESTAMP_NTZ,
            raw_data VARIANT
        )
        """)
        logger.info(f"Table {SNOWFLAKE_TABLE} is ready")
    except Exception as e:
        logger.error(f"Error ensuring table exists: {str(e)}")
        raise
    finally:
        cursor.close()

def process_messages():
    """Process messages from Kafka and store in Snowflake"""
    consumer = create_consumer()
    conn = get_snowflake_connection()
    
    # Ensure the table exists
    ensure_table_exists(conn)
    
    cursor = conn.cursor()
    
    try:
        # Main processing loop
        for message in consumer:
            try:
                weather_data = message.value
                city = weather_data.get('city', 'unknown')
                fetch_timestamp = weather_data.get('fetch_timestamp')
                
                # Serialize to JSON string
                json_str = json.dumps(weather_data)
                
                # Use proper parameter binding with placeholders
                sql = f"""
                INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
                (city, fetch_timestamp, raw_data)
                SELECT 
                    %s, 
                    CURRENT_TIMESTAMP(), 
                    TO_VARIANT(%s)
                """
                
                # Execute with parameter binding
                cursor.execute(sql, (city, json_str))
                
                logger.info(f"Inserted weather data for {city} into Snowflake")
                
                # Commit offset after successful processing
                consumer.commit()
            except Exception as e:
                logger.error(f"Error processing message: {str(e)}")
                logger.error(f"SQL attempted: {sql}")
                logger.error(f"Parameters: city={city}, json_length={len(json_str) if 'json_str' in locals() else 'N/A'}")
                # Continue processing other messages
    except KeyboardInterrupt:
        logger.info("Consumer shutting down...")
    finally:
        cursor.close()
        conn.close()
        consumer.close()



if __name__ == "__main__":
    logger.info("Starting OpenWeatherMap Kafka consumer...")
    process_messages()
