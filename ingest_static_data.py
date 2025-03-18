import os
from dotenv import load_dotenv
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from typing import Dict, List
import json
import glob
from pathlib import Path

# Create logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Add the console handler to the logger
logger.addHandler(console_handler)

# Check if file logging is enabled
if os.getenv('ENABLE_FILE_LOGGING', 'true').strip().lower() in ('true', '1'):
    log_filename = f'logs/ingest_static_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

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

def load_json_config(file_path: str) -> Dict:
    """Load configuration from JSON file"""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading JSON file {file_path}: {e}")
        return {}

def setup_ticket_capacity_table(engine, schema: str):
    """Create ticket capacity table if it doesn't exist"""
    try:
        with engine.connect() as conn:
            setup_sql = read_sql_file('setup_ticket_capacity_configs.sql')
            formatted_sql = setup_sql.format(schema=schema)
            conn.execute(text(formatted_sql))
            conn.commit()
            logger.info(f"Successfully set up ticket capacity table for {schema}")
    except Exception as e:
        logger.error(f"Error setting up ticket capacity table for {schema}: {e}")
        raise

def upsert_ticket_capacity(engine, schema: str, group: str, event_day: str, capacity: int):
    """Insert or update ticket capacity"""
    try:
        with engine.connect() as conn:
            upsert_sql = read_sql_file('upsert_ticket_capacity_config.sql')
            formatted_sql = upsert_sql.format(schema=schema)
            conn.execute(
                text(formatted_sql),
                {
                    "ticket_group": group,
                    "event_day": event_day,
                    "capacity": capacity
                }
            )
            conn.commit()
            logger.info(f"Updated capacity for {group} on {event_day}={capacity} in schema {schema}")
    except Exception as e:
        logger.error(f"Error upserting ticket capacity for schema {schema}: {e}")
        raise

def process_env_configs():
    """Process all environment configurations"""
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

def process_json_configs():
    """Process all JSON configuration files"""
    engine = get_db_engine()
    
    json_files = glob.glob('data_static/schemas/*.json')
    
    for json_file in json_files:
        schema_name = Path(json_file).stem
        config = load_json_config(json_file)
        
        if not config.get('ticket_capacities'):
            logger.warning(f"No ticket capacities found in {json_file}")
            continue
            
        try:
            setup_schema_and_table(engine, schema_name)
            setup_ticket_capacity_table(engine, schema_name)
            
            # Process combined capacities
            if 'all' in config['ticket_capacities']:
                for group, capacity in config['ticket_capacities']['all'].items():
                    upsert_ticket_capacity(engine, schema_name, group, 'ALL', capacity)
            
            # Process day-specific capacities if they exist
            if 'by_day' in config['ticket_capacities']:
                for day, categories in config['ticket_capacities']['by_day'].items():
                    for group, capacity in categories.items():
                        upsert_ticket_capacity(engine, schema_name, group, day, capacity)
                
            logger.info(f"Successfully processed ticket capacities for schema {schema_name}")
            
        except Exception as e:
            logger.error(f"Error processing schema {schema_name}: {e}")
            continue

def main():
    load_dotenv()
    
    # Process both ENV configs and JSON configs
    process_env_configs()  # Your existing ENV processing
    process_json_configs()  # New JSON processing

if __name__ == "__main__":
    main() 