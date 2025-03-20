import os
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from typing import Dict, List, Tuple, Optional
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
    log_filename = f'logs/ingest_age_groups_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

class AgeGroupIngester:
    def __init__(self, schema: str):
        self.schema = schema
        self.engine = self._create_engine()
        
    def _create_engine(self):
        """Create database engine from environment variables"""
        db_url = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        return create_engine(db_url)
    
    def _read_sql_file(self, filename: str) -> str:
        """Read SQL file and format with schema"""
        with open(os.path.join('sql', filename), 'r') as f:
            return f.read().replace('{SCHEMA}', self.schema)
    
    def setup_tables(self):
        """Set up necessary database tables"""
        try:
            with self.engine.connect() as conn:
                # Drop existing table first
                conn.execute(text(f"DROP TABLE IF EXISTS {self.schema}.ticket_age_groups CASCADE;"))
                
                # Create new table
                setup_sql = self._read_sql_file('setup_ticket_age_groups.sql')
                conn.execute(text(setup_sql))
                conn.commit()
                
            logger.info(f"Successfully set up age groups table for {self.schema}")
        except Exception as e:
            logger.error(f"Error setting up age groups table for {self.schema}: {e}")
            raise
    
    def get_age_ranges(self, category: str) -> List[Tuple[str, Optional[int], Optional[int]]]:
        """Get list of age group ranges based on ticket category"""
        if category == "double":
            return [
                ("U29", 0, 29),
                ("30-39", 30, 39),
                ("40-49", 40, 49),
                ("50-59", 50, 59),
                ("60-69", 60, 69),
                ("70+", 70, 999),
                ("Incomplete", None, None)
            ]
        elif category == "relay":
            return [
                ("U40", 0, 39),
                ("40+", 40, 999),
                ("Incomplete", None, None)
            ]
        else:  # single or default
            return [
                ("U24", 0, 24),
                ("25-29", 25, 29),
                ("30-34", 30, 34),
                ("35-39", 35, 39),
                ("40-44", 40, 44),
                ("45-49", 45, 49),
                ("50-54", 50, 54),
                ("55-59", 55, 59),
                ("60-64", 60, 64),
                ("65-69", 65, 69),
                ("70+", 70, 999),
                ("Incomplete", None, None)
            ]

    def process_age_groups(self):
        """Process age groups for all ticket types"""
        try:
            groups_sql = self._read_sql_file('get_ticket_groups.sql')
            count_sql = self._read_sql_file('get_age_group_count.sql')
            upsert_sql = self._read_sql_file('upsert_ticket_age_group.sql')
            
            with self.engine.connect() as conn:
                # Get ticket groups with their categories
                ticket_groups_result = conn.execute(text(groups_sql))
                ticket_groups = [(row[0], row[1]) for row in ticket_groups_result]  # (group, category)
                
                updates = []
                for group, category in ticket_groups:
                    group_total = 0  # Initialize total for each group
                    
                    # Get appropriate age ranges for this category
                    age_ranges = self.get_age_ranges(category.lower())
                    
                    # Process each age range
                    for range_name, min_age, max_age in age_ranges:
                        params = {
                            "ticket_group": group,
                            "min_age": min_age,
                            "max_age": max_age,
                            "is_incomplete": range_name == "Incomplete"
                        }
                        
                        count = conn.execute(text(count_sql), params).scalar()
                        
                        if count > 0:
                            group_total += count
                            updates.append({
                                "ticket_group": group,
                                "age_range": range_name,
                                "count": count
                            })
                    
                    # Add total for this group if there were any counts
                    if group_total > 0:
                        updates.append({
                            "ticket_group": group,
                            "age_range": "Total",
                            "count": group_total
                        })
                
                # Execute all updates
                if updates:
                    for update in updates:
                        conn.execute(text(upsert_sql), update)
                    conn.commit()
                
                logger.info(f"Successfully processed {len(updates)} age group records for {self.schema}")
                
        except Exception as e:
            logger.error(f"Error processing age groups: {e}")
            raise

def process_schemas():
    """Process age groups for all configured schemas"""
    for key in os.environ:
        if key.startswith("EVENT_CONFIGS__") and key.endswith("__schema_name"):
            schema = os.environ[key]
            try:
                logger.info(f"Processing age groups for schema: {schema}")
                ingester = AgeGroupIngester(schema)
                ingester.setup_tables()
                ingester.process_age_groups()
                logger.info(f"Completed processing age groups for {schema}")
            except Exception as e:
                logger.error(f"Failed to process schema {schema}: {e}")
                continue

def main():
    load_dotenv()
    process_schemas()

if __name__ == "__main__":
    main() 