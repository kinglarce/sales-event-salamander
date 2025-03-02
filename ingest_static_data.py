import os
from dotenv import load_dotenv
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from typing import Dict, List

# Create logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create file handler with timestamp in filename
current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
file_handler = logging.FileHandler(f'logs/ingest_static_{current_time}.log')
file_handler.setLevel(logging.INFO)

# Create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add handlers
logger.addHandler(file_handler)
logger.addHandler(console_handler)

def read_sql_file(filename: str) -> str:
    """Read SQL file contents"""
    with open(os.path.join('sql', filename), 'r') as file:
        return file.read()

def get_db_engine():
    """Create database engine from environment variables"""
    db_url = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    return create_engine(db_url)

def setup_schema_and_table(engine, schema: str):
    """Create schema and table if they don't exist"""
    try:
        with engine.connect() as conn:
            # Create schema
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            
            # Read and execute setup SQL
            setup_sql = read_sql_file('setup_event_capacity_configs.sql')
            # Use format() instead of % for Python string formatting
            formatted_sql = setup_sql.format(
                schema=schema,
                trigger_name=f"validate_price_tier_{schema}"
            )
            conn.execute(text(formatted_sql))
            
            conn.commit()
            logger.info(f"Successfully set up schema and table for {schema}")
            
    except Exception as e:
        logger.error(f"Error setting up schema {schema}: {e}")
        raise

def upsert_config(engine, schema: str, category: str, value: str):
    """Insert or update configuration value"""
    try:
        with engine.connect() as conn:
            upsert_sql = read_sql_file('upsert_event_capacity_config.sql')
            # Use format() instead of % for Python string formatting
            formatted_sql = upsert_sql.format(schema=schema)
            conn.execute(
                text(formatted_sql),
                {"category": category, "value": value}
            )
            conn.commit()
            logger.info(f"Updated {category}={value} in schema {schema}")
    except Exception as e:
        logger.error(f"Error upserting config for schema {schema}: {e}")
        raise

def get_event_configs() -> Dict[str, Dict[str, str]]:
    """Get all event configurations from environment"""
    configs = {}
    for key, value in os.environ.items():
        if key.startswith("EVENT_CONFIGS__"):
            parts = key.split("__")
            if len(parts) == 3:
                region = parts[1]
                param = parts[2]
                
                if region not in configs:
                    configs[region] = {"schema_name": None, "configs": {}}
                
                if param == "schema_name":
                    configs[region]["schema_name"] = value
                elif param in ["max_capacity", "start_wave", "price_tier", "price_trigger"]:
                    configs[region]["configs"][param] = value
    
    return configs

def main():
    load_dotenv()
    engine = get_db_engine()
    
    # Get configurations from environment
    configs = get_event_configs()
    
    for region, config in configs.items():
        schema_name = config.get("schema_name")
        if not schema_name:
            logger.warning(f"No schema name found for region {region}, skipping")
            continue
            
        try:
            # Setup schema and table
            setup_schema_and_table(engine, schema_name)
            
            # Update configurations
            for category, value in config.get("configs", {}).items():
                upsert_config(engine, schema_name, category, value)
                
            logger.info(f"Successfully processed all configs for schema {schema_name}")
            
        except Exception as e:
            logger.error(f"Error processing region {region}: {e}")
            continue

if __name__ == "__main__":
    main() 