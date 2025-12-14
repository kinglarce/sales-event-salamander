import logging
import requests
import asyncio
import httpx
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text, func, inspect
from models.database import Base, CouponSeries, Coupon, CouponUsageSummary
import time
from math import ceil
from typing import Dict, Set, List, Tuple, Optional, Union, Any
from dataclasses import dataclass
from enum import Enum
import os
from dotenv import load_dotenv
import re
import csv
import json

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
    log_filename = f'logs/ingest_coupons_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

class VivenuCouponAPI:
    """API implementation for coupon data using httpx"""
    def __init__(self, token: str):
        self.token = token
        self.base_url = os.getenv('EVENT_API_BASE_URL', '').rstrip('/')
        
        # Browser-like headers
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://vivenu.com",
            "Referer": "https://vivenu.com/"
        }
        self._client = None
        self._loop = None
        logger.debug(f"Coupon API initialized with URL: {self.base_url}")
        
    async def _ensure_client(self):
        """Ensure httpx client exists with improved connection settings"""
        if self._client is None:
            # Configure httpx with retry and connection settings for Docker environments
            limits = httpx.Limits(
                max_keepalive_connections=20,
                max_connections=100,
                keepalive_expiry=30.0
            )
            
            # Create timeout configuration with separate connect and read timeouts
            timeout = httpx.Timeout(
                connect=10.0,  # Connection timeout
                read=30.0,     # Read timeout
                write=10.0,    # Write timeout
                pool=5.0       # Pool timeout
            )
            
            self._client = httpx.AsyncClient(
                headers=self.headers,
                verify=False,  # Disable SSL verification for container environments
                timeout=timeout,
                limits=limits,
                # Add retry transport for automatic retries
                transport=httpx.AsyncHTTPTransport(
                    retries=3,  # Retry failed requests up to 3 times
                    http2=False  # Disable HTTP/2 for better Docker compatibility
                )
            )
        return self._client
        
    async def _get_coupon_series_async(self):
        """Async implementation of get_coupon_series using httpx"""
        client = await self._ensure_client()
        url = f"{self.base_url}/coupon/series"
        
        logger.debug(f"Making httpx request to: {url}")
        
        try:
            await asyncio.sleep(0.5)
            response = await client.get(url)
            
            if response.status_code != 200:
                logger.error(f"Error response status: {response.status_code}")
                logger.error(f"Error response body: {response.text}")
                response.raise_for_status()
                
            return response.json()
        except Exception as e:
            logger.error(f"Httpx request failed: {str(e)}")
            raise
            
    async def _get_coupons_async(self, event_id: str, skip: int = 0, limit: int = 1000):
        """Async implementation of get_coupons using httpx"""
        client = await self._ensure_client()
        url = f"{self.base_url}/coupon/rich"
        
        params = {
            "active": "true",
            "skip": skip,
            "top": limit,
            "eventId": event_id
        }
        
        logger.debug(f"Making httpx request to: {url} with params {params}")
        
        try:
            await asyncio.sleep(0.5)
            response = await client.get(url, params=params)
            
            if response.status_code != 200:
                logger.error(f"Error response status: {response.status_code}")
                logger.error(f"Error response body: {response.text}")
                response.raise_for_status()
                
            return response.json()
        except Exception as e:
            logger.error(f"Httpx request failed: {str(e)}")
            raise

    def _get_or_create_loop(self):
        """Get existing loop or create a new one if needed"""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            return loop
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop

    def get_coupon_series(self):
        """Synchronous wrapper for the async method"""
        loop = self._get_or_create_loop()
        try:
            return loop.run_until_complete(self._get_coupon_series_async())
        except RuntimeError as e:
            if str(e) == "Event loop is closed":
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                return loop.run_until_complete(self._get_coupon_series_async())
            raise

    def get_coupons(self, event_id: str, skip: int = 0, limit: int = 1000):
        """Synchronous wrapper for the async method"""
        loop = self._get_or_create_loop()
        try:
            return loop.run_until_complete(self._get_coupons_async(event_id, skip, limit))
        except RuntimeError as e:
            if str(e) == "Event loop is closed":
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                return loop.run_until_complete(self._get_coupons_async(event_id, skip, limit))
            raise
        
    async def close(self):
        """Close the httpx client"""
        if self._client:
            await self._client.aclose()
            self._client = None
            
    def __del__(self):
        """Ensure the client is closed when the object is destroyed"""
        if hasattr(self, '_client') and self._client:
            try:
                loop = self._get_or_create_loop()
                if not loop.is_closed():
                    loop.run_until_complete(self.close())
            except Exception as e:
                logger.debug(f"Error closing httpx client: {str(e)}")

class DatabaseManager:
    def __init__(self, schema: str):
        self.schema = schema
        self.engine = self._create_engine()
        self._session_factory = sessionmaker(bind=self.engine)

    def _create_engine(self):
        """Create database engine from environment variables"""
        db_url = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        return create_engine(db_url)

    def get_session(self):
        """Create a new session for each request"""
        session = self._session_factory()
        session.execute(text(f"SET search_path TO {self.schema}"))
        return session

    def setup_schema(self):
        """Set up schema and tables"""
        with self.engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema}"))
            conn.execute(text(f"SET search_path TO {self.schema}"))
            
            # Drop existing coupon tables
            conn.execute(text(f"""
                -- Drop coupon related tables
                DROP TABLE IF EXISTS {self.schema}.coupon_usage_summary CASCADE;
                DROP TABLE IF EXISTS {self.schema}.coupons CASCADE;
                DROP TABLE IF EXISTS {self.schema}.coupon_series CASCADE;
            """))
                
            conn.commit()

        # Create tables
        Base.metadata.schema = self.schema
        Base.metadata.create_all(self.engine)
        logger.info(f"Successfully set up schema and tables for {self.schema}")

class TransactionManager:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def __enter__(self):
        self.session = self.db_manager.get_session()
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                self.session.commit()
            else:
                self.session.rollback()
        finally:
            self.session.close()

def parse_datetime(dt_str):
    """Parse datetime string to datetime object"""
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        return None

def load_distributed_codes(schema_name: str) -> Dict[str, str]:
    """Load distributed coupon codes and categories from CSV file using schema_name"""
    distributed_codes = {}
    
    # Look for CSV file in data_static/coupons/ directory using schema_name
    csv_path = f"data_static/coupons/{schema_name}-distributed.csv"
    
    if os.path.exists(csv_path):
        try:
            with open(csv_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    if 'Code' in row and row['Code']:
                        code = row['Code'].strip()
                        category = row.get('Category', '').strip()
                        
                        # Clean up category: replace special characters and spaces with hyphens
                        if category:
                            # Replace slash with hyphen and remove extra spaces
                            category = category.replace('/', '-').replace(' ', '-')
                            # Remove any double hyphens
                            category = '-'.join(filter(None, category.split('-')))
                        
                        distributed_codes[code] = category
            logger.info(f"Loaded {len(distributed_codes)} tracked codes with categories from {csv_path}")
        except Exception as e:
            logger.error(f"Error loading tracked codes from {csv_path}: {e}")
    else:
        logger.warning(f"Tracked codes file not found: {csv_path} - proceeding with untracked codes")
    
    return distributed_codes

def create_coupon_series(session: sessionmaker, series_data: dict, schema: str) -> CouponSeries:
    """Create or update coupon series record - simplified"""
    series = CouponSeries(
        id=series_data['_id'],
        region_schema=schema,
        name=series_data.get('name'),
        active=series_data.get('active', True)
    )
    
    try:
        session.merge(series)
        session.commit()
        return series
    except Exception as e:
        session.rollback()
        logger.error(f"Error creating coupon series: {e}")
        raise

def create_coupon(session: sessionmaker, coupon_data: dict, schema: str, tracked_codes: Dict[str, str]) -> Coupon:
    """Create or update coupon record - simplified"""
    coupon_code = coupon_data.get('code', '')
    is_tracked = coupon_code in tracked_codes
    category = tracked_codes.get(coupon_code, '') if is_tracked else None
    is_used = coupon_data.get('used', 0) > 0
    
    coupon = Coupon(
        id=coupon_data['_id'],
        region_schema=schema,
        code=coupon_code,
        name=coupon_data.get('name'),
        active=coupon_data.get('active', True),
        used=coupon_data.get('used', 0),
        is_used=is_used,
        is_tracked=is_tracked,
        category=category,
        coupon_series_id=coupon_data.get('couponSeriesId')
    )
    
    try:
        session.merge(coupon)
        session.commit()
        return coupon
    except Exception as e:
        session.rollback()
        logger.error(f"Error creating coupon: {e}")
        raise

def update_coupon_usage_summary(session, schema: str):
    """Update coupon usage summary for all series"""
    try:
        # Get summary data by series
        summary_query = text("""
            SELECT 
                cs.id as series_id,
                cs.name as series_name,
                COUNT(c.id) as total_codes,
                SUM(CASE WHEN c.is_used THEN 1 ELSE 0 END) as used_codes,
                SUM(CASE WHEN NOT c.is_used THEN 1 ELSE 0 END) as unused_codes,
                SUM(CASE WHEN c.is_tracked THEN 1 ELSE 0 END) as tracked_codes,
                SUM(CASE WHEN c.is_tracked AND c.is_used THEN 1 ELSE 0 END) as tracked_used_codes,
                SUM(CASE WHEN c.is_tracked AND NOT c.is_used THEN 1 ELSE 0 END) as tracked_unused_codes
            FROM {schema}.coupon_series cs
            LEFT JOIN {schema}.coupons c ON cs.id = c.coupon_series_id
            GROUP BY cs.id, cs.name
        """.replace('{schema}', schema))
        
        results = session.execute(summary_query).fetchall()
        
        # Update or create summary records
        for row in results:
            summary_id = f"{row.series_id}_{schema}"
            
            summary = session.get(CouponUsageSummary, summary_id)
            if summary:
                # Update existing summary
                summary.total_codes = row.total_codes or 0
                summary.used_codes = row.used_codes or 0
                summary.unused_codes = row.unused_codes or 0
                summary.tracked_codes = row.tracked_codes or 0
                summary.tracked_used_codes = row.tracked_used_codes or 0
                summary.tracked_unused_codes = row.tracked_unused_codes or 0
                summary.updated_at = datetime.now()
            else:
                # Create new summary
                summary = CouponUsageSummary(
                    id=summary_id,
                    region_schema=schema,
                    series_id=row.series_id,
                    series_name=row.series_name,
                    total_codes=row.total_codes or 0,
                    used_codes=row.used_codes or 0,
                    unused_codes=row.unused_codes or 0,
                    tracked_codes=row.tracked_codes or 0,
                    tracked_used_codes=row.tracked_used_codes or 0,
                    tracked_unused_codes=row.tracked_unused_codes or 0
                )
                session.add(summary)
        
        session.commit()
        logger.info(f"Updated coupon usage summary for schema: {schema}")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error updating coupon usage summary in schema {schema}: {e}")
        raise

class BatchProcessor:
    def __init__(self, batch_size: int = 1000, max_workers: int = 5):
        self.batch_size = batch_size
        self.max_workers = max_workers

    def process_coupons(self, api, db_manager: DatabaseManager, event_id: str, schema: str, tracked_codes: Dict[str, str]) -> int:
        """Process coupons in optimized batches"""
        try:
            # Get the first batch to determine total count
            first_batch = api.get_coupons(event_id, skip=0, limit=1)
            total_coupons = first_batch.get("total", 0)
            
            if not total_coupons:
                logger.warning("No coupons found to process")
                return 0

            total_batches = ceil(total_coupons / self.batch_size)
            processed_total = 0
            logger.info(f"Processing {total_coupons} coupons in {total_batches} batches")

            # Process in chunks to control parallelism
            for chunk_start in range(0, total_batches, self.max_workers):
                chunk_end = min(chunk_start + self.max_workers, total_batches)
                
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = []
                    for batch_num in range(chunk_start, chunk_end):
                        skip = batch_num * self.batch_size
                        futures.append(
                            executor.submit(
                                self._process_coupon_batch, 
                                api.token, 
                                api.base_url,
                                api.headers,
                                db_manager, 
                                event_id,
                                schema, 
                                tracked_codes,
                                batch_num, 
                                skip, 
                                total_batches,
                                self.batch_size
                            )
                        )
                    
                    # Process results
                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            if isinstance(result, int):
                                processed_total += result
                        except Exception as e:
                            logger.error(f"Batch processing error: {str(e)}")

            return processed_total
            
        except Exception as e:
            logger.error(f"Error in process_coupons: {str(e)}")
            raise
            
    def _process_coupon_batch(self, token, base_url, headers, db_manager, event_id, schema, tracked_codes, batch_num, skip, total_batches, batch_size):
        """Process a single batch using httpx in its own thread and event loop"""
        try:
            # Create a new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                # Run the async processing
                return loop.run_until_complete(
                    self._process_single_coupon_batch(token, base_url, headers, db_manager, event_id, schema, tracked_codes, batch_num, skip, total_batches, batch_size)
                )
            finally:
                # Always clean up the event loop
                loop.close()
                
        except Exception as e:
            logger.error(f"Error processing batch {batch_num + 1}/{total_batches}: {str(e)}")
            raise

    async def _process_single_coupon_batch(self, token, base_url, headers, db_manager, event_id, schema, tracked_codes, batch_num, skip, total_batches, batch_size):
        """Process a single batch with a fresh httpx client"""
        # Create a new httpx client for this batch only with improved settings
        limits = httpx.Limits(
            max_keepalive_connections=20,
            max_connections=100,
            keepalive_expiry=30.0
        )
        
        timeout = httpx.Timeout(
            connect=10.0,
            read=30.0,
            write=10.0,
            pool=5.0
        )
        
        async with httpx.AsyncClient(
            headers=headers, 
            verify=False, 
            timeout=timeout,
            limits=limits,
            transport=httpx.AsyncHTTPTransport(
                retries=3,
                http2=False
            )
        ) as client:
            try:
                # Fetch the coupons
                url = f"{base_url}/coupon/rich"
                params = {
                    "active": "true",
                    "skip": skip,
                    "top": batch_size,
                    "eventId": event_id
                }
                
                # Add a small delay to avoid rate limiting
                await asyncio.sleep(0.5)
                
                response = await client.get(url, params=params)
                
                if response.status_code != 200:
                    logger.error(f"Error response status: {response.status_code}")
                    logger.error(f"Error response body: {response.text}")
                    response.raise_for_status()
                
                coupon_data = response.json()
                coupons = coupon_data.get("rows", [])
                
                if not coupons:
                    logger.warning(f"No coupons found in batch {batch_num + 1}/{total_batches}")
                    return 0
                
                # Process the batch in a blocking transaction
                with TransactionManager(db_manager) as session:
                    processed = 0
                    for coupon in coupons:
                        try:
                            create_coupon(session, coupon, schema, tracked_codes)
                            processed += 1
                        except Exception as e:
                            logger.error(f"Failed to process coupon {coupon.get('_id')}: {str(e)}")
                            continue
                    
                    logger.info(
                        f"Batch {batch_num + 1}/{total_batches} complete. "
                        f"Processed: {processed}/{len(coupons)} coupons."
                    )
                    return processed
                    
            except Exception as e:
                logger.error(f"Error processing batch {batch_num + 1}/{total_batches}: {str(e)}")
                raise

def ingest_coupon_data(token: str, event_id: str, schema: str, debug: bool = False):
    """Main coupon ingestion function"""
    if debug:
        logger.setLevel(logging.DEBUG)
    
    db_manager = DatabaseManager(schema)
    
    try:
        # Setup schema and tables
        db_manager.setup_schema()

        # Load tracked codes using schema_name
        tracked_codes = load_distributed_codes(schema)
        logger.info(f"Loaded {len(tracked_codes)} tracked codes for schema {schema}")

        # Initialize API
        api = None
        api_type = None
        
        try:
            logger.info("Using httpx implementation for coupon API access")
            api = VivenuCouponAPI(token)
            api_type = "httpx"
        except Exception as e:
            logger.error(f"Failed to initialize coupon API: {str(e)}")
            raise
        
        if not api:
            logger.error("Failed to initialize API")
            return

        # Process coupon series
        logger.info("Fetching coupon series data...")
        series_data = api.get_coupon_series()
        
        with TransactionManager(db_manager) as session:
            for series in series_data.get("docs", []):
                try:
                    create_coupon_series(session, series, schema)
                except Exception as e:
                    logger.error(f"Failed to process series {series.get('_id')}: {str(e)}")
                    continue
        
        logger.info(f"Processed {len(series_data.get('docs', []))} coupon series")

        # Process individual coupons with optimized batching
        batch_processor = BatchProcessor(batch_size=1000, max_workers=5)
        processed_count = batch_processor.process_coupons(api, db_manager, event_id, schema, tracked_codes)
        
        if processed_count > 0:
            # Update usage summary in final transaction
            with TransactionManager(db_manager) as session:
                update_coupon_usage_summary(session, schema)
                
            logger.info(f"Successfully processed {processed_count} coupons for event {event_id}")

    except Exception as e:
        logger.error(f"Error during coupon ingestion for schema {schema}: {str(e)}", exc_info=True)
        raise
    finally:
        # Ensure the API session is properly closed
        try:
            if api and hasattr(api, 'close') and api_type == "httpx":
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(api.close())
                except Exception as e:
                    logger.debug(f"Error closing API session: {str(e)}")
        except Exception as e:
            logger.debug(f"Error during API cleanup: {str(e)}")

def get_event_configs():
    """Get all event configurations from environment"""
    from collections import defaultdict
    
    configs = defaultdict(dict)
    for key, value in os.environ.items():
        if key.startswith("EVENT_CONFIGS__"):
            _, region, param = key.split("__", 2)
            if param in ["token", "event_id", "schema_name"]:
                configs[region][param] = value
            configs[region]["region"] = region

    return [
        {
            "token": config["token"],
            "event_id": config["event_id"],
            "schema": config["schema_name"],
            "region": config["region"]
        }
        for config in configs.values()
        if all(k in config for k in ["token", "event_id", "schema_name", "region"])
    ]

if __name__ == "__main__":
    load_dotenv()
    
    # Add command line argument for debug mode
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()
    
    configs = get_event_configs()
    if not configs:
        raise ValueError("No valid event configurations found in environment")
    
    # Set up main event loop
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    # Process each config
    for config in configs:
        try:
            logger.info(f"Processing coupon data for schema: {config['schema']}")
            ingest_coupon_data(
                config["token"], 
                config["event_id"], 
                config["schema"], 
                debug=args.debug
            )
        except Exception as e:
            logger.error(f"Failed to process schema {config['schema']}: {e}")
            continue 
            
    # Clean up event loop
    try:
        pending = asyncio.all_tasks(loop)
        if pending:
            loop.run_until_complete(asyncio.gather(*pending))
    except Exception:
        pass 