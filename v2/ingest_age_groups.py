"""
Age Groups Ingestion v2
Refactored with senior software engineering best practices.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from core import (
    get_config,
    get_logger,
    DatabaseManager,
    TransactionManager,
    PerformanceLogger
)

logger = get_logger(__name__)


@dataclass
class AgeGroupResult:
    """Result of age group processing"""
    schema: str
    region: str
    processed_groups: int
    total_updates: int
    duration: float
    success: bool
    error_message: Optional[str] = None


class AgeGroupIngester:
    """Handles age group analysis and processing"""
    
    def __init__(self, config, db_manager: DatabaseManager):
        self.config = config
        self.db_manager = db_manager
        self.logger = get_logger(__name__)
        self.performance_logger = PerformanceLogger(self.logger)
    
    def get_age_ranges(self, category: str) -> List[Tuple[str, Optional[int], Optional[int]]]:
        """Get age ranges based on category"""
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
        elif category in ["relay", "corporate_relay"]:
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
    
    def _read_sql_file(self, filename: str, schema: str) -> str:
        """Read SQL file and replace schema placeholder"""
        with open(f'sql/{filename}', 'r') as f:
            return f.read().replace('{SCHEMA}', schema)
    
    def setup_tables(self, schema: str):
        """Set up age groups table"""
        try:
            with TransactionManager(self.db_manager) as session:
                # Drop and recreate table
                session.execute(f"DROP TABLE IF EXISTS {schema}.ticket_age_groups CASCADE")
                
                setup_sql = self._read_sql_file('setup_ticket_age_groups.sql', schema)
                session.execute(setup_sql)
                
            self.logger.info(f"Successfully set up age groups table for {schema}")
        except Exception as e:
            self.logger.error(f"Error setting up age groups table for {schema}: {e}")
            raise
    
    def get_ticket_groups(self, schema: str) -> List[Tuple[str, str, str, str]]:
        """Get all ticket groups with their category and event day"""
        try:
            groups_sql = self._read_sql_file('get_ticket_groups.sql', schema)
            with TransactionManager(self.db_manager) as session:
                result = session.execute(groups_sql)
                return [(row[0], row[1], row[2], row[3]) for row in result]
        except Exception as e:
            self.logger.error(f"Error getting ticket groups: {e}")
            return []
    
    def process_batch(self, batch: List[Tuple[str, str, str, str]], batch_index: int, 
                     total_batches: int, schema: str) -> List[Dict]:
        """Process a batch of ticket groups"""
        try:
            count_sql = self._read_sql_file('get_age_group_count.sql', schema)
            updates = []
            
            with TransactionManager(self.db_manager) as session:
                for display_name, category, group, event_day in batch:
                    self.logger.debug(f"Batch {batch_index+1}/{total_batches}: Processing group: {display_name}")
                    
                    total = 0
                    for range_name, min_age, max_age in self.get_age_ranges(category.lower()):
                        params = {
                            "ticket_group": group,
                            "event_day": event_day,
                            "min_age": min_age,
                            "max_age": max_age,
                            "is_incomplete": range_name == "Incomplete"
                        }
                        
                        result = session.execute(count_sql, params).fetchone()
                        count, group_total, _, _, sql_category = result
                        used_category = sql_category if sql_category else category
                        
                        if count is not None:
                            updates.append({
                                "ticket_group": group,
                                "ticket_event_day": event_day.upper(),
                                "age_range": range_name,
                                "count": count,
                                "ticket_category": used_category
                            })
                            total = group_total
                    
                    if total > 0:
                        updates.append({
                            "ticket_group": group,
                            "ticket_event_day": event_day.upper(),
                            "age_range": "Total",
                            "count": total,
                            "ticket_category": category
                        })
            
            return updates
            
        except Exception as e:
            self.logger.error(f"Error processing batch {batch_index+1}/{total_batches}: {e}")
            return []
    
    async def process_age_groups(self, schema: str, region: str, 
                                max_workers: int = 4, batch_size: int = 20) -> AgeGroupResult:
        """Process age groups for a schema"""
        start_time = time.time()
        
        try:
            self.performance_logger.start_timer(f"age_groups_{schema}")
            
            # Setup tables
            self.setup_tables(schema)
            
            # Get ticket groups
            ticket_groups = self.get_ticket_groups(schema)
            if not ticket_groups:
                self.logger.warning(f"No ticket groups found for {schema}")
                return AgeGroupResult(
                    schema=schema,
                    region=region,
                    processed_groups=0,
                    total_updates=0,
                    duration=time.time() - start_time,
                    success=True
                )
            
            self.logger.info(f"Found {len(ticket_groups)} ticket group combinations for {schema}")
            
            # Split into batches
            batches = []
            for i in range(0, len(ticket_groups), batch_size):
                batches.append(ticket_groups[i:i + batch_size])
            
            total_batches = len(batches)
            self.logger.info(f"Processing {len(ticket_groups)} ticket groups in {total_batches} batches")
            
            all_updates = []
            
            # Process batches in parallel
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_batch = {
                    executor.submit(self.process_batch, batch, i, total_batches, schema): i 
                    for i, batch in enumerate(batches)
                }
                
                for future in as_completed(future_to_batch):
                    batch_index = future_to_batch[future]
                    try:
                        batch_updates = future.result()
                        all_updates.extend(batch_updates)
                        self.logger.info(f"Batch {batch_index+1}/{total_batches} completed with {len(batch_updates)} updates")
                    except Exception as e:
                        self.logger.error(f"Batch {batch_index+1}/{total_batches} failed: {e}")
            
            # Execute all updates
            if all_updates:
                upsert_sql = self._read_sql_file('upsert_ticket_age_group.sql', schema)
                with TransactionManager(self.db_manager) as session:
                    for update in all_updates:
                        session.execute(upsert_sql, update)
                
                self.logger.info(f"Successfully processed {len(all_updates)} age group records for {schema}")
            
            duration = time.time() - start_time
            self.performance_logger.end_timer(f"age_groups_{schema}")
            
            return AgeGroupResult(
                schema=schema,
                region=region,
                processed_groups=len(ticket_groups),
                total_updates=len(all_updates),
                duration=duration,
                success=True
            )
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"Error processing age groups for {schema}: {e}")
            self.performance_logger.end_timer(f"age_groups_{schema}", success=False)
            
            return AgeGroupResult(
                schema=schema,
                region=region,
                processed_groups=0,
                total_updates=0,
                duration=duration,
                success=False,
                error_message=str(e)
            )


async def main_ingest_age_groups():
    """Main entry point for age groups ingestion"""
    config = get_config()
    db_manager = DatabaseManager()
    ingester = AgeGroupIngester(config, db_manager)
    
    results = []
    
    # Process each event configuration
    for event_config in config.events:
        try:
            result = await ingester.process_age_groups(
                schema=event_config.schema,
                region=event_config.region,
                max_workers=config.age_groups.max_workers,
                batch_size=config.age_groups.batch_size
            )
            results.append(result)
            
            if result.success:
                logger.info(f"Age groups processed for {event_config.schema}: {result.total_updates} updates")
            else:
                logger.error(f"Failed to process age groups for {event_config.schema}: {result.error_message}")
                
        except Exception as e:
            logger.error(f"Error processing age groups for {event_config.schema}: {e}")
            continue
    
    return results


if __name__ == "__main__":
    asyncio.run(main_ingest_age_groups())
