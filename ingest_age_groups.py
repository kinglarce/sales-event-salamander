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
    def __init__(self, schema: str, region: str):
        self.schema = schema
        self.region = region
        self.engine = self._create_engine()
        self.summary_by_day = os.getenv(
            f"EVENT_CONFIGS__{region}__summary_breakdown_day", "false"
        ).strip().lower() in ('true', '1')
        logger.info(f"Initialized age group ingester for schema {schema}, region: {region}, summary_by_day: {self.summary_by_day}")
        
    def _create_engine(self):
        db_url = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        return create_engine(db_url)
    
    def _read_sql_file(self, filename: str) -> str:
        with open(os.path.join('sql', filename), 'r') as f:
            return f.read().replace('{SCHEMA}', self.schema)
    
    def setup_tables(self):
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {self.schema}.ticket_age_groups CASCADE;"))
                setup_sql = self._read_sql_file('setup_ticket_age_groups.sql')
                conn.execute(text(setup_sql))
                conn.commit()
            logger.info(f"Successfully set up age groups table for {self.schema}")
        except Exception as e:
            logger.error(f"Error setting up age groups table for {self.schema}: {e}")
            raise
    
    def get_age_ranges(self, category: str) -> List[Tuple[str, Optional[int], Optional[int]]]:
        """Get age ranges based on category, ensuring proper group size handling"""
        if category == "double":
            return [
                ("U29", 0, 29),
                ("30-39", 30, 39),
                ("40-49", 40, 49),
                ("50-59", 50, 59),
                ("60-69", 60, 69),
                ("70+", 70, 999),
                ("Incomplete", None, None),
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

    def get_ticket_groups(self) -> List[Tuple[str, str, str, str]]:
        """Get all ticket groups with their category and event day"""
        try:
            groups_sql = self._read_sql_file('get_ticket_groups.sql')
            with self.engine.connect() as conn:
                result = conn.execute(text(groups_sql))
                # Returns: display_ticket_group, category, ticket_group, ticket_event_day
                return [(row[0], row[1], row[2], row[3]) for row in result]
        except Exception as e:
            logger.error(f"Error getting ticket groups: {e}")
            return []

    def process_age_groups(self):
        try:
            upsert_sql = self._read_sql_file('upsert_ticket_age_group.sql')
            count_sql = self._read_sql_file('get_age_group_count.sql')
            
            ticket_groups = self.get_ticket_groups()
            logger.info(f"Found {len(ticket_groups)} ticket group combinations")
            
            with self.engine.begin() as conn:
                updates = []
                
                for display_name, category, group, event_day in ticket_groups:
                    logger.debug(f"Processing group: {display_name} (category: {category})")
                    
                    total = 0
                    # Process all age ranges including incomplete
                    for range_name, min_age, max_age in self.get_age_ranges(category.lower()):
                        params = {
                            "ticket_group": group,
                            "event_day": event_day,
                            "min_age": min_age,
                            "max_age": max_age,
                            "is_incomplete": range_name == "Incomplete"
                        }
                        
                        result = conn.execute(text(count_sql), params).fetchone()
                        # Unpack all results: count, total, incomplete_txns, complete_txns, ticket_category
                        count, group_total, _, _, sql_category = result
                        
                        # Use the category from the SQL if it exists, otherwise use the one from ticket_groups
                        # This ensures consistency with how category is determined in the SQL
                        used_category = sql_category if sql_category else category
                        
                        if count is not None:
                            updates.append({
                                "ticket_group": group,
                                "ticket_event_day": event_day.upper(),
                                "age_range": range_name,
                                "count": count,
                                "ticket_category": used_category  # Use the determined category
                            })
                            total = group_total  # Update total from any valid count
                    
                    # Add total after processing all ranges
                    if total > 0:
                        updates.append({
                            "ticket_group": group,
                            "ticket_event_day": event_day.upper(),
                            "age_range": "Total",
                            "count": total,
                            "ticket_category": category  # Use the category from ticket_groups for totals
                        })
                
                # Execute all updates
                if updates:
                    for update in updates:
                        conn.execute(text(upsert_sql), update)
            
            logger.info(f"Successfully processed {len(updates)} age group records for {self.schema}")
            
        except Exception as e:
            logger.error(f"Error processing age groups: {e}", exc_info=True)
            raise

def process_schemas():
    load_dotenv()
    
    processed_schemas = []
    
    for key in os.environ:
        if key.startswith("EVENT_CONFIGS__") and key.endswith("__schema_name"):
            region = key.split("__")[1]
            schema = os.environ[key]
            
            # Skip if already processed
            if schema in processed_schemas:
                continue
                
            try:
                logger.info(f"Processing age groups for schema: {schema}, region: {region}")
                ingester = AgeGroupIngester(schema, region)
                ingester.setup_tables()
                ingester.process_age_groups()
                logger.info(f"Completed processing age groups for {schema}")
                processed_schemas.append(schema)
            except Exception as e:
                logger.error(f"Failed to process schema {schema}: {e}")
                continue

if __name__ == "__main__":
    process_schemas() 