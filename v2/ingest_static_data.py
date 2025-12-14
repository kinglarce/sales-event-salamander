"""
Static Data Ingestion v2
Refactored with senior software engineering best practices.
"""

import sys
import os
import asyncio
import logging
import json
import glob
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from pathlib import Path
import pandas as pd

# Add project root to Python path for shared components
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from v2.core import (
    get_config,
    get_logger,
    DatabaseManager,
    TransactionManager,
    PerformanceLogger
)

logger = get_logger(__name__)


@dataclass
class StaticDataResult:
    """Result of static data processing"""
    schema: str
    region: str
    processed_configs: int
    processed_capacities: int
    processed_countries: int
    duration: float
    success: bool
    error_message: Optional[str] = None


class StaticDataIngester:
    """Handles static data ingestion (configs, capacities, countries)"""
    
    def __init__(self, config, db_manager: DatabaseManager):
        self.config = config
        self.db_manager = db_manager
        self.logger = get_logger(__name__)
        self.performance_logger = PerformanceLogger(self.logger)
    
    def _read_sql_file(self, filename: str) -> str:
        """Read SQL file contents"""
        with open(f'sql/{filename}', 'r') as f:
            return f.read()
    
    def setup_schema_and_table(self, schema: str):
        """Create schema and table if they don't exist"""
        try:
            with TransactionManager(self.db_manager) as session:
                # Create schema
                session.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                
                # Read and execute setup SQL
                setup_sql = self._read_sql_file('setup_event_capacity_configs.sql')
                formatted_sql = setup_sql.format(
                    schema=schema,
                    trigger_name=f"validate_price_tier_{schema}"
                )
                session.execute(formatted_sql)
                
            self.logger.info(f"Successfully set up schema and table for {schema}")
            
        except Exception as e:
            self.logger.error(f"Error setting up schema {schema}: {e}")
            raise
    
    def upsert_config(self, schema: str, category: str, value: str):
        """Insert or update configuration value"""
        try:
            with TransactionManager(self.db_manager) as session:
                upsert_sql = self._read_sql_file('upsert_event_capacity_config.sql')
                formatted_sql = upsert_sql.format(schema=schema)
                session.execute(formatted_sql, {"category": category, "value": value})
                
            self.logger.debug(f"Updated {category}={value} in schema {schema}")
        except Exception as e:
            self.logger.error(f"Error upserting config for schema {schema}: {e}")
            raise
    
    def setup_ticket_capacity_table(self, schema: str):
        """Create ticket capacity table if it doesn't exist"""
        try:
            with TransactionManager(self.db_manager) as session:
                setup_sql = self._read_sql_file('setup_ticket_capacity_configs.sql')
                formatted_sql = setup_sql.format(schema=schema)
                session.execute(formatted_sql)
                
            self.logger.info(f"Successfully set up ticket capacity table for {schema}")
        except Exception as e:
            self.logger.error(f"Error setting up ticket capacity table for {schema}: {e}")
            raise
    
    def upsert_ticket_capacity(self, schema: str, group: str, event_day: str, capacity: int):
        """Insert or update ticket capacity"""
        try:
            with TransactionManager(self.db_manager) as session:
                upsert_sql = self._read_sql_file('upsert_ticket_capacity_config.sql')
                formatted_sql = upsert_sql.format(schema=schema)
                session.execute(formatted_sql, {
                    "ticket_group": group,
                    "event_day": event_day,
                    "capacity": capacity
                })
                
            self.logger.debug(f"Updated capacity for {group} on {event_day}={capacity} in schema {schema}")
        except Exception as e:
            self.logger.error(f"Error upserting ticket capacity for schema {schema}: {e}")
            raise
    
    def setup_country_table(self, schema: str):
        """Create country configs table if it doesn't exist"""
        try:
            with TransactionManager(self.db_manager) as session:
                setup_sql = self._read_sql_file('setup_country_configs.sql')
                formatted_sql = setup_sql.format(schema=schema)
                session.execute(formatted_sql)
                
            self.logger.info(f"Successfully set up country configs table for {schema}")
        except Exception as e:
            self.logger.error(f"Error setting up country configs table for {schema}: {e}")
            raise
    
    def get_region_for_country(self, country_code: str, regions_data: Dict) -> Tuple[str, str]:
        """Get region and sub-region for a country code"""
        for region, sub_regions in regions_data["regions"].items():
            for sub_region, countries in sub_regions.items():
                if country_code in countries:
                    return region, sub_region
        return "Other", "Other"
    
    def upsert_country_config(self, schema: str, code: str, country: str, region: str, sub_region: str):
        """Insert or update country configuration"""
        try:
            with TransactionManager(self.db_manager) as session:
                upsert_sql = self._read_sql_file('upsert_country_config.sql')
                formatted_sql = upsert_sql.format(schema=schema)
                session.execute(formatted_sql, {
                    "code": code,
                    "country": country,
                    "region": region,
                    "sub_region": sub_region
                })
                
            self.logger.debug(f"Updated country config for {code} ({country}) in schema {schema}")
        except Exception as e:
            self.logger.error(f"Error upserting country config for {code} in schema {schema}: {e}")
            raise
    
    def load_json_config(self, file_path: str) -> Dict:
        """Load configuration from JSON file"""
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Error loading JSON file {file_path}: {e}")
            return {}
    
    async def process_env_configs(self) -> Dict[str, int]:
        """Process all environment configurations"""
        results = {"configs": 0, "schemas": 0}
        
        try:
            # Get configurations from environment
            configs = {}
            for key, value in self.config.environment.items():
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
            
            for region, config in configs.items():
                schema_name = config.get("schema_name")
                if not schema_name:
                    self.logger.warning(f"No schema name found for region {region}, skipping")
                    continue
                    
                try:
                    # Setup schema and table
                    self.setup_schema_and_table(schema_name)
                    
                    # Update configurations
                    for category, value in config.get("configs", {}).items():
                        self.upsert_config(schema_name, category, value)
                        results["configs"] += 1
                        
                    results["schemas"] += 1
                    self.logger.info(f"Successfully processed all configs for schema {schema_name}")
                    
                except Exception as e:
                    self.logger.error(f"Error processing region {region}: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error processing environment configs: {e}")
            
        return results
    
    async def process_json_configs(self) -> Dict[str, int]:
        """Process all JSON configuration files"""
        results = {"capacities": 0, "schemas": 0}
        
        try:
            json_files = glob.glob('data_static/schemas/*.json')
            
            for json_file in json_files:
                schema_name = Path(json_file).stem
                # Fix schema names that start with numbers (PostgreSQL requirement)
                if schema_name[0].isdigit():
                    schema_name = f"schema_{schema_name}"
                config = self.load_json_config(json_file)
                
                if not config.get('ticket_capacities'):
                    self.logger.warning(f"No ticket capacities found in {json_file}")
                    continue
                    
                try:
                    self.setup_schema_and_table(schema_name)
                    self.setup_ticket_capacity_table(schema_name)
                    
                    # Process combined capacities
                    if 'all' in config['ticket_capacities']:
                        for group, capacity in config['ticket_capacities']['all'].items():
                            self.upsert_ticket_capacity(schema_name, group, 'ALL', capacity)
                            results["capacities"] += 1
                    
                    # Process day-specific capacities if they exist
                    if 'by_day' in config['ticket_capacities']:
                        for day, categories in config['ticket_capacities']['by_day'].items():
                            for group, capacity in categories.items():
                                self.upsert_ticket_capacity(schema_name, group, day, capacity)
                                results["capacities"] += 1
                                
                    results["schemas"] += 1
                    self.logger.info(f"Successfully processed ticket capacities for schema {schema_name}")
                    
                except Exception as e:
                    self.logger.error(f"Error processing schema {schema_name}: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error processing JSON configs: {e}")
            
        return results
    
    async def process_country_data(self) -> Dict[str, int]:
        """Process country data from CSV and regions from JSON"""
        results = {"countries": 0, "schemas": 0}
        
        try:
            # Load regions data
            with open('data_static/country_regions.json', 'r') as f:
                regions_data = json.load(f)
            
            # Load country data
            country_data = pd.read_csv('data_static/countries.csv')
            
            # Process country data for each schema
            for event_config in self.config.events:
                schema_name = event_config.schema
                try:
                    # Setup table
                    self.setup_country_table(schema_name)
                    
                    # Process each country
                    for _, row in country_data.iterrows():
                        region, sub_region = self.get_region_for_country(str(row['Code']), regions_data)
                        self.upsert_country_config(
                            schema_name,
                            str(row['Code']),
                            str(row['Country']),
                            region,
                            sub_region
                        )
                        results["countries"] += 1
                        
                    results["schemas"] += 1
                    self.logger.info(f"Successfully processed country data for schema {schema_name}")
                    
                except Exception as e:
                    self.logger.error(f"Error processing country data for schema {schema_name}: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error processing country data: {e}")
            
        return results
    
    async def process_static_data(self) -> StaticDataResult:
        """Main static data processing function"""
        start_time = datetime.now()
        
        try:
            self.performance_logger.start_timer("static_data_ingestion")
            
            # Process all types of configurations
            env_results = await self.process_env_configs()
            json_results = await self.process_json_configs()
            country_results = await self.process_country_data()
            
            duration = (datetime.now() - start_time).total_seconds()
            self.performance_logger.end_timer("static_data_ingestion")
            
            return StaticDataResult(
                schema="all",
                region="all",
                processed_configs=env_results["configs"],
                processed_capacities=json_results["capacities"],
                processed_countries=country_results["countries"],
                duration=duration,
                success=True
            )
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"Error processing static data: {e}")
            self.performance_logger.end_timer("static_data_ingestion", success=False)
            
            return StaticDataResult(
                schema="all",
                region="all",
                processed_configs=0,
                processed_capacities=0,
                processed_countries=0,
                duration=duration,
                success=False,
                error_message=str(e)
            )


async def main_ingest_static_data():
    """Main entry point for static data ingestion"""
    config = get_config()
    db_manager = DatabaseManager(config.database)
    ingester = StaticDataIngester(config, db_manager)
    
    result = await ingester.process_static_data()
    
    if result.success:
        logger.info(f"Static data ingestion completed:")
        logger.info(f"  Configs: {result.processed_configs}")
        logger.info(f"  Capacities: {result.processed_capacities}")
        logger.info(f"  Countries: {result.processed_countries}")
        logger.info(f"  Duration: {result.duration:.2f}s")
    else:
        logger.error(f"Static data ingestion failed: {result.error_message}")
    
    return result


if __name__ == "__main__":
    asyncio.run(main_ingest_static_data())
