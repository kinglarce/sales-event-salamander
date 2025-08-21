import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import httpx
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Load environment variables
load_dotenv()

# =============================================================================
# CONFIGURATION - EASILY MODIFIABLE
# =============================================================================

# Event configuration - read from .env file (agnostic to region)
REGION = "australia"
EVENT_ID = os.getenv(f"EVENT_CONFIGS__{REGION}__event_id")

# Validate required environment variables
if not EVENT_ID:
    raise ValueError("EVENT_ID environment variable is required. Please set it in your .env file.")

# =============================================================================
# END CONFIGURATION
# =============================================================================

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/gender_update.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Additional imports needed for the script
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text, func, inspect
from models.database import Base, Event, Ticket
import time
from math import ceil
from typing import Dict, Set, List, Tuple, Optional, Union, Any
from dataclasses import dataclass
from enum import Enum
import re

class UpdateStatus(Enum):
    """Status of gender field updates"""
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"

@dataclass
class UpdateResult:
    """Result of a gender field update"""
    ticket_id: str
    ticket_name: str
    status: UpdateStatus
    error_message: Optional[str] = None
    response_status: Optional[int] = None
    response_body: Optional[str] = None
    updated_at: Optional[datetime] = None

class VivenuSyncAPI:
    """Synchronous API implementation using httpx for updating tickets"""
    
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
        logger.debug(f"API initialized with URL: {self.base_url}")
        
    def update_ticket_gender(self, ticket_id: str, update_data: Dict) -> Tuple[bool, Optional[str], Optional[int], Optional[str]]:
        """
        Update a ticket's gender field via PUT request (synchronous)
        
        Args:
            ticket_id: The ticket ID to update
            update_data: The data to send in the PUT request
            
        Returns:
            Tuple of (success, error_message, response_status, response_body)
        """
        import httpx
        
        url = f"{self.base_url}/tickets/{ticket_id}"
        
        logger.debug(f"Making PUT request to: {url}")
        logger.debug(f"Update data: {json.dumps(update_data, indent=2)}")
        
        try:
            # Use synchronous httpx client
            with httpx.Client(
                headers=self.headers,
                verify=False,
                timeout=30.0
            ) as client:
                # Add a small delay to avoid rate limiting
                import time
                time.sleep(0.5)
                
                response = client.put(url, json=update_data)
                
                if response.status_code == 200:
                    logger.info(f"✅ Successfully updated ticket {ticket_id}")
                    return True, None, response.status_code, response.text
                else:
                    logger.error(f"❌ Failed to update ticket {ticket_id}: {response.status_code}")
                    logger.error(f"Response body: {response.text}")
                    return False, f"HTTP {response.status_code}: {response.text}", response.status_code, response.text
                    
        except Exception as e:
            error_msg = f"Request failed: {str(e)}"
            logger.error(f"❌ Error updating ticket {ticket_id}: {error_msg}")
            return False, error_msg, None, None

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

    def setup_update_tracking_table(self):
        """Set up a table for tracking update progress and results"""
        with self.engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema}"))
            conn.execute(text(f"SET search_path TO {self.schema}"))
            
            # Create a table to track update progress
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.gender_update_tracking (
                    ticket_id VARCHAR PRIMARY KEY,
                    ticket_name VARCHAR,
                    update_status VARCHAR DEFAULT 'pending',
                    error_message TEXT,
                    response_status INTEGER,
                    response_body TEXT,
                    attempts INTEGER DEFAULT 0,
                    last_attempt_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Add update tracking columns to the existing analysis table if they don't exist
            try:
                conn.execute(text(f"""
                    ALTER TABLE {self.schema}.tickets_gender_analysis 
                    ADD COLUMN IF NOT EXISTS update_status VARCHAR DEFAULT 'pending',
                    ADD COLUMN IF NOT EXISTS update_error_message TEXT,
                    ADD COLUMN IF NOT EXISTS update_response_status INTEGER,
                    ADD COLUMN IF NOT EXISTS update_response_body TEXT,
                    ADD COLUMN IF NOT EXISTS update_attempts INTEGER DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS last_update_attempt TIMESTAMP,
                    ADD COLUMN IF NOT EXISTS last_updated_at TIMESTAMP
                """))
            except Exception as e:
                logger.debug(f"Columns may already exist: {e}")
            
            conn.commit()
        logger.info(f"Successfully set up update tracking for schema {self.schema}")

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

class GenderUpdateProcessor:
    """Processes gender field updates and tracks results"""
    
    def __init__(self, session, schema: str):
        self.session = session
        self.schema = schema
        self.processed = 0
        self.successful = 0
        self.failed = 0
        self.skipped = 0

    def load_payloads_from_file(self, payload_file: str) -> List[Dict]:
        """Load update payloads from a JSON file"""
        try:
            with open(payload_file, 'r') as f:
                payloads = json.load(f)
            
            logger.info(f"Loaded {len(payloads)} payloads from {payload_file}")
            return payloads
            
        except FileNotFoundError:
            logger.error(f"Payload file not found: {payload_file}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in payload file: {e}")
            raise
        except Exception as e:
            logger.error(f"Error loading payloads: {e}")
            raise

    def validate_payload(self, payload: Dict) -> bool:
        """Validate that a payload has all required fields"""
        required_fields = ['ticket_id', 'update_data']
        
        for field in required_fields:
            if field not in payload:
                logger.error(f"Missing required field '{field}' in payload")
                return False
                
        if 'extraFields' not in payload['update_data']:
            logger.error(f"Missing 'extraFields' in update_data")
            return False
            
        return True

    def track_update_progress(self, ticket_id: str, ticket_name: str, status: str = 'pending'):
        """Track update progress in the database"""
        try:
            # Update the tracking table
            upsert_sql = text(f"""
                INSERT INTO {self.schema}.gender_update_tracking 
                (ticket_id, ticket_name, update_status, created_at, updated_at)
                VALUES (:ticket_id, :ticket_name, :status, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (ticket_id) DO UPDATE SET
                    ticket_name = EXCLUDED.ticket_name,
                    update_status = EXCLUDED.update_status,
                    updated_at = CURRENT_TIMESTAMP
            """)
            
            self.session.execute(upsert_sql, {
                'ticket_id': ticket_id,
                'ticket_name': ticket_name,
                'status': status
            })
            
        except Exception as e:
            logger.error(f"Error tracking update progress for ticket {ticket_id}: {str(e)}")
            raise

    def update_tracking_result(self, ticket_id: str, result: UpdateResult):
        """Update the tracking table with the result of an update attempt"""
        try:
            # Update the tracking table
            update_sql = text(f"""
                UPDATE {self.schema}.gender_update_tracking 
                SET 
                    update_status = :status,
                    error_message = :error_message,
                    response_status = :response_status,
                    response_body = :response_body,
                    attempts = attempts + 1,
                    last_attempt_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE ticket_id = :ticket_id
            """)
            
            self.session.execute(update_sql, {
                'ticket_id': ticket_id,
                'status': result.status.value,
                'error_message': result.error_message,
                'response_status': result.response_status,
                'response_body': result.response_body
            })
            
            # Also update the analysis table
            analysis_update_sql = text(f"""
                UPDATE {self.schema}.tickets_gender_analysis 
                SET 
                    update_status = :status,
                    update_error_message = :error_message,
                    update_response_status = :response_status,
                    update_response_body = :response_body,
                    update_attempts = update_attempts + 1,
                    last_update_attempt = CURRENT_TIMESTAMP,
                    last_updated_at = CASE 
                        WHEN :status = 'success' THEN CURRENT_TIMESTAMP 
                        ELSE last_updated_at 
                    END
                WHERE id = :ticket_id
            """)
            
            self.session.execute(analysis_update_sql, {
                'ticket_id': ticket_id,
                'status': result.status.value,
                'error_message': result.error_message,
                'response_status': result.response_status,
                'response_body': result.response_body
            })
            
        except Exception as e:
            logger.error(f"Error updating tracking result for ticket {ticket_id}: {str(e)}")
            raise

    def get_update_summary(self) -> Dict:
        """Get summary of update results"""
        try:
            # Get summary from tracking table
            result = self.session.execute(
                text(f"""
                    SELECT 
                        update_status,
                        COUNT(*) as count
                    FROM {self.schema}.gender_update_tracking
                    GROUP BY update_status
                    ORDER BY update_status
                """)
            )
            status_summary = {row.update_status: row.count for row in result}
            
            # Get total processed
            result = self.session.execute(
                text(f"SELECT COUNT(*) as total FROM {self.schema}.gender_update_tracking")
            )
            total_processed = result.scalar()
            
            # Get recent errors
            result = self.session.execute(
                text(f"""
                    SELECT 
                        ticket_id,
                        ticket_name,
                        error_message,
                        last_attempt_at
                    FROM {self.schema}.gender_update_tracking
                    WHERE update_status = 'failed'
                    ORDER BY last_attempt_at DESC
                    LIMIT 10
                """)
            )
            recent_errors = [
                {
                    'ticket_id': row.ticket_id,
                    'ticket_name': row.ticket_name,
                    'error_message': row.error_message,
                    'last_attempt_at': row.last_attempt_at
                }
                for row in result
            ]
            
            return {
                'total_processed': total_processed,
                'status_summary': status_summary,
                'recent_errors': recent_errors
            }
            
        except Exception as e:
            logger.error(f"Error getting update summary: {e}")
            return {}

class BatchUpdateProcessor:
    def __init__(self, batch_size: int = 10, max_workers: int = 3):
        self.batch_size = batch_size
        self.max_workers = max_workers

    def process_updates(self, api: VivenuSyncAPI, db_manager: DatabaseManager, 
                       schema: str, payloads: List[Dict], dry_run: bool = False) -> List[UpdateResult]:
        """Process gender field updates in batches"""
        
        if dry_run:
            logger.info("🔍 DRY RUN MODE - No actual API calls will be made")
        
        results = []
        total_payloads = len(payloads)
        
        logger.info(f"Processing {total_payloads} gender field updates")
        logger.info(f"Batch size: {self.batch_size}, Max workers: {self.max_workers}")
        
        # Process in batches
        for batch_start in range(0, total_payloads, self.batch_size):
            batch_end = min(batch_start + self.batch_size, total_payloads)
            batch_payloads = payloads[batch_start:batch_end]
            
            logger.info(f"Processing batch {batch_start//self.batch_size + 1}/{(total_payloads + self.batch_size - 1)//self.batch_size}")
            logger.info(f"Batch range: {batch_start + 1}-{batch_end} of {total_payloads}")
            
            # Process batch with ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                
                for payload in batch_payloads:
                    if not self.validate_payload(payload):
                        logger.warning(f"Skipping invalid payload: {payload.get('ticket_id', 'unknown')}")
                        continue
                        
                    futures.append(
                        executor.submit(
                            self._process_single_update,
                            api,
                            db_manager,
                            schema,
                            payload,
                            dry_run
                        )
                    )
                
                # Collect results
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            results.append(result)
                    except Exception as e:
                        logger.error(f"Error processing update: {str(e)}")
            
            # Small delay between batches
            if batch_end < total_payloads:
                time.sleep(1)
        
        return results

    def validate_payload(self, payload: Dict) -> bool:
        """Validate that a payload has all required fields"""
        required_fields = ['ticket_id', 'update_data']
        
        for field in required_fields:
            if field not in payload:
                logger.error(f"Missing required field '{field}' in payload")
                return False
                
        if 'extraFields' not in payload['update_data']:
            logger.error(f"Missing 'extraFields' in update_data")
            return False
            
        return True

    def _process_single_update(self, api: VivenuSyncAPI, db_manager: DatabaseManager, 
                              schema: str, payload: Dict, dry_run: bool) -> Optional[UpdateResult]:
        """Process a single gender field update"""
        
        ticket_id = payload['ticket_id']
        ticket_name = payload.get('ticket_name', 'Unknown')
        update_data = payload['update_data']
        
        logger.info(f"Processing update for ticket: {ticket_id} ({ticket_name})")
        
        # Track progress in database
        with TransactionManager(db_manager) as session:
            processor = GenderUpdateProcessor(session, schema)
            processor.track_update_progress(ticket_id, ticket_name, 'pending')
        
        if dry_run:
            logger.info(f"🔍 DRY RUN: Would update ticket {ticket_id} with data: {json.dumps(update_data, indent=2)}")
            
            result = UpdateResult(
                ticket_id=ticket_id,
                ticket_name=ticket_name,
                status=UpdateStatus.SKIPPED,
                error_message="Dry run mode - no actual update performed"
            )
        else:
            try:
                # Send the actual API update using synchronous method
                success, error_msg, response_status, response_body = api.update_ticket_gender(
                    ticket_id, update_data
                )
                
                if success:
                    result = UpdateResult(
                        ticket_id=ticket_id,
                        ticket_name=ticket_name,
                        status=UpdateStatus.SUCCESS,
                        response_status=response_status,
                        response_body=response_body,
                        updated_at=datetime.now()
                    )
                else:
                    result = UpdateResult(
                        ticket_id=ticket_id,
                        ticket_name=ticket_name,
                        status=UpdateStatus.FAILED,
                        error_message=error_msg,
                        response_status=response_status,
                        response_body=response_body
                    )
                    
            except Exception as e:
                error_msg = f"Unexpected error: {str(e)}"
                logger.error(f"Error updating ticket {ticket_id}: {error_msg}")
                
                result = UpdateResult(
                    ticket_id=ticket_id,
                    ticket_name=ticket_name,
                    status=UpdateStatus.FAILED,
                    error_message=error_msg
                )
        
        # Update tracking in database
        with TransactionManager(db_manager) as session:
            processor = GenderUpdateProcessor(session, schema)
            processor.update_tracking_result(ticket_id, result)
        
        return result

def update_gender_fields(token: str, schema: str, payload_file: str, 
                        dry_run: bool = False, debug: bool = False):
    """Main function to update gender fields"""
    
    if debug:
        logger.setLevel(logging.DEBUG)
    
    db_manager = DatabaseManager(schema)
    
    try:
        # Set up update tracking
        db_manager.setup_update_tracking_table()
        
        # Initialize API
        api = VivenuSyncAPI(token)
        
        # Load payloads
        processor = GenderUpdateProcessor(None, schema)
        payloads = processor.load_payloads_from_file(payload_file)
        
        if not payloads:
            logger.warning("No payloads to process")
            return []
        
        # Process updates
        batch_processor = BatchUpdateProcessor(batch_size=10, max_workers=3)
        results = batch_processor.process_updates(api, db_manager, schema, payloads, dry_run)
        
        # Get final summary
        with TransactionManager(db_manager) as session:
            processor.session = session
            summary = processor.get_update_summary()
            
            # Print summary
            logger.info(f"Gender Update Summary for schema {schema}:")
            logger.info(f"  Total processed: {summary.get('total_processed', 0)}")
            
            if summary.get('status_summary'):
                logger.info("  Status breakdown:")
                for status, count in summary['status_summary'].items():
                    logger.info(f"    {status}: {count}")
            
            if summary.get('recent_errors'):
                logger.info("  Recent errors:")
                for error in summary['recent_errors'][:5]:  # Show first 5
                    logger.info(f"    {error['ticket_name']} ({error['ticket_id']}): {error['error_message']}")
            
            return results

    except Exception as e:
        logger.error(f"Error during gender field updates for schema {schema}: {str(e)}", exc_info=True)
        raise
    finally:
        # No cleanup needed for synchronous API
        pass

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
    
    # Add command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Update gender fields in Vivenu tickets')
    parser.add_argument('--payload-file', required=True, help='Path to the payload JSON file')
    parser.add_argument('--schema', help='Process specific schema only')
    parser.add_argument('--dry-run', action='store_true', help='Dry run mode (no actual API calls)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()
    
    # Validate payload file exists (check both direct path and /data directory)
    payload_file_path = args.payload_file
    
    # If just filename provided, check in /data directory first
    if not os.path.dirname(payload_file_path):
        data_path = os.path.join("data", payload_file_path)
        if os.path.exists(data_path):
            payload_file_path = data_path
            logger.info(f"Found payload file in /data directory: {payload_file_path}")
        elif os.path.exists(payload_file_path):
            logger.info(f"Using payload file from current directory: {payload_file_path}")
        else:
            logger.error(f"Payload file not found: {args.payload_file}")
            logger.error(f"Checked locations:")
            logger.error(f"  - /data/{args.payload_file}")
            logger.error(f"  - {os.path.abspath(args.payload_file)}")
            sys.exit(1)
    else:
        # Full path provided, check if it exists
        if not os.path.exists(payload_file_path):
            logger.error(f"Payload file not found: {payload_file_path}")
            sys.exit(1)
    
    # Update args.payload_file to use the resolved path
    args.payload_file = payload_file_path
    
    configs = get_event_configs()
    if not configs:
        raise ValueError("No valid event configurations found in environment")
    
    # Filter configs if specific schema requested
    if args.schema:
        configs = [config for config in configs if config['schema'] == args.schema]
        if not configs:
            raise ValueError(f"No configuration found for schema {args.schema}")
    
    # Set up main event loop
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    # Process each config
    for config in configs:
        try:
            logger.info(f"Processing schema: {config['schema']} for gender field updates")
            logger.info(f"Payload file: {args.payload_file}")
            logger.info(f"Dry run mode: {args.dry_run}")
            
            results = update_gender_fields(
                config["token"], 
                config["schema"], 
                args.payload_file,
                dry_run=args.dry_run,
                debug=args.debug
            )
            
            if results:
                logger.info(f"Successfully processed schema {config['schema']}")
                logger.info(f"  Total results: {len(results)}")
                
                # Count by status
                status_counts = {}
                for result in results:
                    status = result.status.value
                    status_counts[status] = status_counts.get(status, 0) + 1
                
                for status, count in status_counts.items():
                    logger.info(f"  {status}: {count}")
            else:
                logger.warning(f"No results generated for schema {config['schema']}")
                
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