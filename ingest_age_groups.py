import os
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from typing import Dict, List, Tuple, Optional
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

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
    def __init__(self, schema: str, region: str, max_workers: int = 5, batch_size: int = 10):
        self.schema = schema
        self.region = region
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.engine = self._create_engine()
        self.summary_by_day = os.getenv(
            f"EVENT_CONFIGS__{region}__summary_breakdown_day", "false"
        ).strip().lower() in ('true', '1')
        logger.info(f"Initialized age group ingester for schema {schema}, region: {region}, summary_by_day: {self.summary_by_day}, max_workers: {max_workers}, batch_size: {batch_size}")
        
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

    def process_batch(self, batch: List[Tuple[str, str, str, str]], batch_index: int, total_batches: int) -> List[Dict]:
        """Process a batch of ticket groups and return a list of updates to be executed"""
        try:
            engine = self._create_engine()  # Create a fresh engine for each batch
            count_sql = self._read_sql_file('get_age_group_count.sql')
            
            start_time = time.time()
            updates = []
            
            with engine.begin() as conn:
                for display_name, category, group, event_day in batch:
                    logger.debug(f"Batch {batch_index+1}/{total_batches}: Processing group: {display_name} (category: {category})")
                    
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
            
            engine.dispose()  # Clean up the engine
            elapsed = time.time() - start_time
            logger.info(f"Batch {batch_index+1}/{total_batches} processed {len(batch)} ticket groups with {len(updates)} updates in {elapsed:.2f}s")
            return updates
            
        except Exception as e:
            logger.error(f"Error processing batch {batch_index+1}/{total_batches}: {str(e)}", exc_info=True)
            return []

    def process_age_groups(self):
        try:
            upsert_sql = self._read_sql_file('upsert_ticket_age_group.sql')
            
            ticket_groups = self.get_ticket_groups()
            if not ticket_groups:
                logger.warning("No ticket groups found to process")
                return
                
            logger.info(f"Found {len(ticket_groups)} ticket group combinations to process")
            
            # Split ticket groups into batches
            batches = []
            for i in range(0, len(ticket_groups), self.batch_size):
                batches.append(ticket_groups[i:i + self.batch_size])
            
            total_batches = len(batches)
            logger.info(f"Processing {len(ticket_groups)} ticket groups in {total_batches} batches with {self.max_workers} workers")
            
            all_updates = []
            start_time = time.time()
            
            # Process batches in parallel
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_batch = {
                    executor.submit(self.process_batch, batch, i, total_batches): i 
                    for i, batch in enumerate(batches)
                }
                
                for future in as_completed(future_to_batch):
                    batch_index = future_to_batch[future]
                    try:
                        batch_updates = future.result()
                        all_updates.extend(batch_updates)
                        logger.info(f"Batch {batch_index+1}/{total_batches} completed successfully with {len(batch_updates)} updates")
                    except Exception as e:
                        logger.error(f"Batch {batch_index+1}/{total_batches} failed: {str(e)}")
            
            # Execute all updates in a single transaction
            if all_updates:
                logger.info(f"Executing {len(all_updates)} total updates to database")
                with self.engine.begin() as conn:
                    for update in all_updates:
                        conn.execute(text(upsert_sql), update)
                
                elapsed = time.time() - start_time
                logger.info(f"Successfully processed {len(all_updates)} age group records for {self.schema} in {elapsed:.2f}s")
            else:
                logger.warning("No updates to execute after batch processing")
            
        except Exception as e:
            logger.error(f"Error processing age groups: {e}", exc_info=True)
            raise

def process_schemas():
    load_dotenv()
    
    processed_schemas = []
    
    # Get max_workers and batch_size from environment or use defaults
    max_workers = int(os.getenv('AGE_GROUP_MAX_WORKERS', '4'))
    batch_size = int(os.getenv('AGE_GROUP_BATCH_SIZE', '20'))
    
    for key in os.environ:
        if key.startswith("EVENT_CONFIGS__") and key.endswith("__schema_name"):
            region = key.split("__")[1]
            schema = os.environ[key]
            
            # Skip if already processed
            if schema in processed_schemas:
                continue
                
            try:
                logger.info(f"Processing age groups for schema: {schema}, region: {region}")
                ingester = AgeGroupIngester(schema, region, max_workers=max_workers, batch_size=batch_size)
                ingester.setup_tables()
                ingester.process_age_groups()
                logger.info(f"Completed processing age groups for {schema}")
                processed_schemas.append(schema)
            except Exception as e:
                logger.error(f"Failed to process schema {schema}: {e}")
                continue

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Process age groups with parallel processing')
    parser.add_argument('--max-workers', type=int, default=4, help='Maximum number of worker threads')
    parser.add_argument('--batch-size', type=int, default=20, help='Number of ticket groups to process in each batch')
    
    args = parser.parse_args()
    
    # Override environment variables with command line arguments
    os.environ['AGE_GROUP_MAX_WORKERS'] = str(args.max_workers)
    os.environ['AGE_GROUP_BATCH_SIZE'] = str(args.batch_size)
    
    process_schemas()