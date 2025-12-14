import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urljoin

import httpx
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Load environment variables
load_dotenv()

# =============================================================================
# MANUAL CONFIGURATION - EASILY MODIFIABLE
# =============================================================================

# Event configuration - read from .env file (agnostic to region)
REGION = "australia"
EVENT_ID = os.getenv(f"EVENT_CONFIGS__{REGION}__event_id")

# Validate required environment variables
if not EVENT_ID:
    raise ValueError("EVENT_ID environment variable is required. Please set it in your .env file.")

# Event day filter - only process tickets for this specific day
EVENT_DAY = "FRIDAY"  # Change to "SATURDAY" or "SUNDAY" as needed

# =============================================================================
# END CONFIGURATION
# =============================================================================

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/gender_fix.log'),
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
from utils.event_processor import determine_ticket_group, determine_ticket_event_day, TicketCategory, TicketEventDay
import time
from math import ceil
from typing import Dict, Set, List, Tuple, Optional, Union, Any
from dataclasses import dataclass
from enum import Enum
import re

class GenderDeterminer:
    """Determines gender based on ticket names using the same logic as the SQL reports"""
    
    @staticmethod
    def determine_gender_from_ticket_name(ticket_name: str) -> Optional[str]:
        """
        Determine gender from ticket name using the same logic as get_detailed_summary_with_day_report.sql
        
        Args:
            ticket_name: The ticket name to analyze
            
        Returns:
            'Female' for women's tickets, 'Male' for men's tickets, None for mixed/neutral tickets
        """
        if not ticket_name:
            return None
            
        name_lower = ticket_name.lower()
        
        # Check for women's tickets first (more specific)
        if any(keyword in name_lower for keyword in [
            'women', 'woman', 'womens', 'womens'
        ]):
            return 'Female'
            
        # Check for men's tickets
        if any(keyword in name_lower for keyword in [
            'men', 'mens', 'man'
        ]):
            return 'Male'
            
        # Check for mixed categories (these should not have gender assigned)
        if any(keyword in name_lower for keyword in [
            'mixed', 'doubles mixed', 'corporate relay mixed'
        ]):
            return None
            
        # Check for relay categories
        if 'relay' in name_lower:
            if 'women' in name_lower or 'womens' in name_lower:
                return 'Female'
            elif 'men' in name_lower or 'mens' in name_lower:
                return 'Male'
            else:
                return None  # Mixed relay
                
        # Check for doubles categories
        if 'doubles' in name_lower:
            if 'women' in name_lower or 'womens' in name_lower:
                return 'Female'
            elif 'men' in name_lower or 'mens' in name_lower:
                return 'Male'
            else:
                return None  # Mixed doubles
                
        # Check for corporate relay categories
        if 'corporate relay' in name_lower:
            if 'women' in name_lower or 'womens' in name_lower:
                return 'Female'
            elif 'men' in name_lower or 'mens' in name_lower:
                return 'Male'
            else:
                return None  # Mixed corporate relay
                
        # Default to None for unclear cases
        return None

class VivenuHttpxAPI:
    """API implementation using httpx for collecting tickets and extra_fields"""
    
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
        logger.debug(f"API initialized with URL: {self.base_url}")
        
    async def _ensure_client(self):
        """Ensure httpx client exists"""
        if self._client is None:
            self._client = httpx.AsyncClient(
                headers=self.headers,
                verify=False,
                timeout=30.0
            )
        return self._client
        
    async def _get_tickets_async(self, skip: int = 0, limit: int = 1000):
        """Async implementation of get_tickets using httpx"""
        client = await self._ensure_client()
        url = f"{self.base_url}/tickets"
        
        params = {
            "status": "VALID,DETAILSREQUIRED",
            "skip": skip,
            "top": limit
        }
        
        logger.debug(f"Making httpx request to: {url} with params {params}")
        
        try:
            # Add a small delay to avoid rate limiting
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

    def get_tickets(self, skip: int = 0, limit: int = 1000):
        """Synchronous wrapper for the async method"""
        loop = self._get_or_create_loop()
        try:
            return loop.run_until_complete(self._get_tickets_async(skip, limit))
        except RuntimeError as e:
            if str(e) == "Event loop is closed":
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                return loop.run_until_complete(self._get_tickets_async(skip, limit))
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

    def setup_gender_analysis_table(self):
        """Set up a temporary table for storing tickets with missing gender for analysis"""
        with self.engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema}"))
            conn.execute(text(f"SET search_path TO {self.schema}"))
            
            # Drop the existing table if it exists to start fresh
            conn.execute(text(f"DROP TABLE IF EXISTS {self.schema}.tickets_gender_analysis CASCADE"))
            
            # Create a fresh table for gender analysis
            conn.execute(text(f"""
                CREATE TABLE {self.schema}.tickets_gender_analysis (
                    id VARCHAR PRIMARY KEY,
                    ticket_name VARCHAR,
                    extra_fields JSONB,
                    determined_gender VARCHAR,
                    needs_update BOOLEAN DEFAULT FALSE,
                    current_gender VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    update_status VARCHAR DEFAULT 'pending',
                    update_error_message TEXT,
                    update_response_status INTEGER,
                    update_response_body TEXT,
                    update_attempts INTEGER DEFAULT 0,
                    last_update_attempt TIMESTAMP,
                    last_updated_at TIMESTAMP
                )
            """))
            
            conn.commit()
        logger.info(f"Successfully dropped and recreated gender analysis table for {self.schema} - starting fresh")

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

class GenderFixProcessor:
    """Processes tickets to identify and fix missing gender fields"""
    
    def __init__(self, session, schema: str):
        self.session = session
        self.schema = schema
        self.processed = 0
        self.failed = 0
        self.gender_determined = 0
        self.needs_update = 0
        self.gender_mapper = GenderDeterminer()

    def is_valid_athlete_ticket(self, ticket: Dict) -> bool:
        """
        Check if ticket is a valid HYROX athlete ticket that should be processed.
        Uses existing event_processor utilities for consistency.
        
        Args:
            ticket: The ticket data from API
            
        Returns:
            bool: True if ticket should be processed, False otherwise
        """
        try:
            ticket_name = ticket.get('ticketName', '')
            event_id = ticket.get('eventId')  # Use eventId like ingest_events_tickets.py

            if not ticket_name:
                logger.debug(f"Ticket has no name, skipping")
                return False

            # First filter: Check if ticket is from the specified event
            if event_id != EVENT_ID:
                logger.debug(f"Skipping ticket from different event: {event_id} (expected: {EVENT_ID})")
                return False
            
            # Second filter: Check if ticket is for the specified event day
            ticket_event_day = determine_ticket_event_day(ticket_name).value
            if ticket_event_day.upper() != EVENT_DAY.upper():
                return False
            
            # Third filter: Use existing event_processor to determine if it's an athlete ticket
            ticket_category = determine_ticket_group(ticket_name)
            # Only process athlete tickets (exclude EXTRA and SPECTATOR)
            if ticket_category in [TicketCategory.EXTRA, TicketCategory.SPECTATOR]:
                return False
            
            # Check if it's a HYROX ticket (should start with HYROX)
            if not ticket_name.upper().startswith('HYROX'):
                return False
            
            logger.debug(f"✅ Valid athlete ticket: {ticket_name} (category: {ticket_category.value})")
            return True
            
        except Exception as e:
            logger.error(f"Error checking ticket validity: {e}")
            return False

    def process_ticket_for_gender_analysis(self, ticket: Dict) -> Optional[Dict]:
        """
        Process a single ticket for gender analysis.
        
        Args:
            ticket: Ticket data from API
            
        Returns:
            Dict with analysis data or None if ticket should be skipped
        """
        try:
            # Extract basic ticket info
            ticket_id = ticket.get('_id')
            ticket_name = ticket.get('ticketName', '')
            extra_fields = ticket.get('extraFields', {})
            
            # Skip if no ticket name
            if not ticket_name:
                logger.warning(f"Ticket {ticket_id} has no name, skipping")
                return None
            
            # Apply ticket filtering - only process valid athlete tickets
            if not self.is_valid_athlete_ticket(ticket):
                return None
            
            # Determine gender from ticket name
            determined_gender = self.gender_mapper.determine_gender_from_ticket_name(ticket_name)
            
            # Get current gender from extra_fields (if it exists)
            current_gender = extra_fields.get('gender')
            
            # Check if update is needed
            needs_update = (
                current_gender is None or 
                current_gender == '' or 
                current_gender != determined_gender
            )
            
            if needs_update:
                logger.debug(f"Ticket {ticket_id} needs gender update: {current_gender} -> {determined_gender}")
            
            return {
                'id': ticket_id,
                'ticket_name': ticket_name,
                'extra_fields': extra_fields,
                'determined_gender': determined_gender,
                'needs_update': needs_update,
                'current_gender': current_gender
            }
            
        except Exception as e:
            logger.error(f"Error processing ticket {ticket.get('_id', 'unknown')}: {e}")
            return None

    def store_gender_analysis(self, analysis_data: Dict):
        """Store gender analysis data in the database"""
        try:
            # Insert or update the analysis record
            insert_sql = text(f"""
                INSERT INTO {self.schema}.tickets_gender_analysis 
                (id, ticket_name, extra_fields, determined_gender, needs_update, current_gender)
                VALUES (:id, :ticket_name, :extra_fields, :determined_gender, :needs_update, :current_gender)
                ON CONFLICT (id) DO UPDATE SET
                    ticket_name = EXCLUDED.ticket_name,
                    extra_fields = EXCLUDED.extra_fields,
                    determined_gender = EXCLUDED.determined_gender,
                    needs_update = EXCLUDED.needs_update,
                    current_gender = EXCLUDED.current_gender,
                    created_at = CURRENT_TIMESTAMP
            """)
            
            self.session.execute(insert_sql, {
                'id': analysis_data['id'],
                'ticket_name': analysis_data['ticket_name'],
                'extra_fields': json.dumps(analysis_data['extra_fields']),
                'determined_gender': analysis_data['determined_gender'],
                'needs_update': analysis_data['needs_update'],
                'current_gender': analysis_data['current_gender']
            })
            
        except Exception as e:
            logger.error(f"Error storing gender analysis for ticket {analysis_data['id']}: {str(e)}")
            raise

    def get_tickets_needing_gender_update(self) -> List[Dict]:
        """Get all tickets that need gender updates"""
        try:
            query = text(f"""
                SELECT 
                    id,
                    ticket_name,
                    extra_fields,
                    determined_gender,
                    current_gender
                FROM {self.schema}.tickets_gender_analysis
                WHERE needs_update = TRUE
                ORDER BY ticket_name
            """)
            
            results = self.session.execute(query).fetchall()
            
            tickets_for_update = []
            for row in results:
                tickets_for_update.append({
                    'id': row[0],
                    'ticket_name': row[1],
                    'extra_fields': row[2] if isinstance(row[2], dict) else json.loads(row[2]),
                    'determined_gender': row[3],
                    'current_gender': row[4]
                })
                
            return tickets_for_update
            
        except Exception as e:
            logger.error(f"Error getting tickets needing gender update: {str(e)}")
            return []

    def generate_update_payloads(self, tickets_for_update: List[Dict]) -> List[Dict]:
        """Generate payloads for PUT requests to update gender fields (for demonstration only)"""
        payloads = []
        
        # Define the exact field order as specified
        field_order = [
            "impairment",
            "first_name", 
            "last_name",
            "athletes_e_mail_address",
            "athletes_phone_number",
            "spectators_e_mail_address",
            "region_of_residence",
            "place_of_residence",
            "zip_code",
            "birth_date",
            "gender",
            "nationality",
            "first_name_of_guide_runner",
            "last_name_of_guide_runner",
            "phone_of_guide_runner",
            "emergency_contact_full_name",
            "emergency_contact_phone_number",
            "expected_finisher_time",
            "hyrox_training_clubs",
            "gym_club_community",
            "local_territory_training",
            "local_territory_training_club",
            "region_training",
            "hyrox_training_club_other_territory_name",
            "f45_studio",
            "relay_team_name",
            "returning_athlete",
            "returning_athlete_city",
            "confirmation_doubles_mixed_team",
            "confirmation_relay_mixed_team",
            "confirmation_rebooking_rules",
            "confirmation_tc",
            "confirmation_waiver",
            "confirmation_waiver_spectator",
            "confirmation_u18_waiver",
            "waiver_signature",
            "adaptive_release",
            "confirmation_privacy_policy",
            "confirmation_newsletter",
            "confirmation_commercial_opt_in",
            "confirmation_partner_opt_in",
            "acknowledgment_of_ticket_terms_flex_or_non_flex"
        ]
        
        for ticket in tickets_for_update:
            # Get the original extraFields structure from the API
            original_extra_fields = ticket['extra_fields']
            
            # Create a new ordered dictionary following the specified order
            updated_extra_fields = {}
            
            # First, add fields in the specified order (if they exist)
            for field_name in field_order:
                if field_name in original_extra_fields:
                    if field_name == 'gender':
                        updated_extra_fields[field_name] = ticket['determined_gender']
                    else:
                        updated_extra_fields[field_name] = original_extra_fields[field_name]
                elif field_name == 'gender':
                    # Gender field is missing from original, add it with determined value
                    updated_extra_fields[field_name] = ticket['determined_gender']
            
            # Then, add any remaining fields that weren't in the specified order
            for field_name, value in original_extra_fields.items():
                if field_name not in field_order:
                    updated_extra_fields[field_name] = value
            
            payload = {
                'ticket_id': ticket['id'],
                'ticket_name': ticket['ticket_name'],
                'current_gender': ticket['current_gender'],
                'determined_gender': ticket['determined_gender'],
                'original_extra_fields': original_extra_fields,  # Show what was originally there
                'updated_extra_fields': updated_extra_fields,   # Show what it will become
                'update_data': {
                    'extraFields': updated_extra_fields  # This is what gets sent to API
                }
            }
            
            payloads.append(payload)
            
        return payloads

    def get_gender_analysis_summary(self) -> Dict:
        """Get summary of gender analysis results"""
        try:
            # Get total processed tickets
            result = self.session.execute(
                text(f"SELECT COUNT(*) as total FROM {self.schema}.tickets_gender_analysis")
            )
            total_processed = result.scalar()
            
            # Get tickets needing updates
            result = self.session.execute(
                text(f"SELECT COUNT(*) as total FROM {self.schema}.tickets_gender_analysis WHERE needs_update = true")
            )
            tickets_needing_update = result.scalar()
            
            # Get gender breakdown
            result = self.session.execute(
                text(f"""
                    SELECT determined_gender, COUNT(*) as count 
                    FROM {self.schema}.tickets_gender_analysis 
                    GROUP BY determined_gender 
                    ORDER BY count DESC
                """)
            )
            gender_breakdown = {row.determined_gender: row.count for row in result}
            
            # Get sample tickets needing updates
            result = self.session.execute(
                text(f"""
                    SELECT ticket_name, current_gender, determined_gender 
                    FROM {self.schema}.tickets_gender_analysis 
                    WHERE needs_update = true 
                    LIMIT 10
                """)
            )
            sample_tickets = [
                {
                    'ticket_name': row.ticket_name,
                    'current_gender': row.current_gender,
                    'determined_gender': row.determined_gender
                }
                for row in result
            ]
            
            return {
                'total_tickets_processed': total_processed,
                'tickets_needing_update': tickets_needing_update,
                'gender_breakdown': gender_breakdown,
                'sample_tickets_needing_update': sample_tickets,
                'filtering_info': {
                    'event_id': EVENT_ID,
                    'event_day': EVENT_DAY,
                    'description': f"Only processing HYROX athlete tickets for {EVENT_DAY} from event {EVENT_ID} using event_processor utilities"
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting summary: {e}")
            return {}

    def clear_analysis_table(self):
        """Clear the gender analysis table"""
        try:
            self.session.execute(text(f"TRUNCATE TABLE {self.schema}.tickets_gender_analysis"))
            logger.info(f"Cleared gender analysis table for schema {self.schema}")
        except Exception as e:
            logger.error(f"Error clearing gender analysis table: {str(e)}")
            raise

    def drop_analysis_table(self):
        """Drop the gender analysis table completely"""
        try:
            self.session.execute(text(f"DROP TABLE IF EXISTS {self.schema}.tickets_gender_analysis CASCADE"))
            logger.info(f"Dropped gender analysis table for schema {self.schema}")
        except Exception as e:
            logger.error(f"Error dropping gender analysis table: {str(e)}")
            raise

def process_batch_for_gender_analysis(session, tickets: List, schema: str):
    """Process batch of tickets for gender analysis"""
    processor = GenderFixProcessor(session, schema)
    
    for ticket in tickets:
        try:
            analysis_data = processor.process_ticket_for_gender_analysis(ticket)
            if analysis_data:
                processor.store_gender_analysis(analysis_data)
        except Exception as e:
            logger.error(f"Failed to process ticket {ticket.get('_id')}: {str(e)}")
            continue
    
    logger.info(f"Batch summary - Processed: {processor.processed}, Failed: {processor.failed}, Gender Determined: {processor.gender_determined}, Needs Update: {processor.needs_update}")
    return processor

class BatchProcessor:
    def __init__(self, batch_size: int = 1000, max_workers: int = 5):
        self.batch_size = batch_size
        self.max_workers = max_workers

    def process_tickets_for_gender_analysis(self, api, db_manager: DatabaseManager, schema: str) -> GenderFixProcessor:
        """Process tickets in optimized batches for gender analysis"""
        try:
            # Get the first batch to determine total count
            first_batch = api.get_tickets(skip=0, limit=1)
            total_tickets = first_batch.get("total", 0)
            
            if not total_tickets:
                logger.warning("No tickets found to process")
                return None

            total_batches = ceil(total_tickets / self.batch_size)
            logger.info(f"Processing {total_tickets} tickets in {total_batches} batches for gender analysis")

            # Process in chunks to control parallelism
            for chunk_start in range(0, total_batches, self.max_workers):
                chunk_end = min(chunk_start + self.max_workers, total_batches)
                
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = []
                    for batch_num in range(chunk_start, chunk_end):
                        skip = batch_num * self.batch_size
                        futures.append(
                            executor.submit(
                                self._process_gender_analysis_batch, 
                                api.token, 
                                api.base_url,
                                api.headers,
                                db_manager, 
                                schema, 
                                batch_num, 
                                skip, 
                                total_batches,
                                self.batch_size
                            )
                        )
                    
                    # Process results
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            logger.error(f"Batch processing error: {str(e)}")

            # Get final summary from the database
            with TransactionManager(db_manager) as session:
                processor = GenderFixProcessor(session, schema)
                return processor

        except Exception as e:
            logger.error(f"Error in process_tickets_for_gender_analysis: {str(e)}")
            raise
            
    def _process_gender_analysis_batch(self, token, base_url, headers, db_manager, schema, batch_num, skip, total_batches, batch_size):
        """Process a single batch for gender analysis"""
        try:
            # Create a new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                # Run the async processing
                return loop.run_until_complete(
                    self._process_single_gender_analysis_batch(token, base_url, headers, db_manager, schema, batch_num, skip, total_batches, batch_size)
                )
            finally:
                # Always clean up the event loop
                loop.close()
                
        except Exception as e:
            logger.error(f"Error processing batch {batch_num + 1}/{total_batches}: {str(e)}")
            raise

    async def _process_single_gender_analysis_batch(self, token, base_url, headers, db_manager, schema, batch_num, skip, total_batches, batch_size):
        """Process a single batch for gender analysis"""
        async with httpx.AsyncClient(headers=headers, verify=False, timeout=30.0) as client:
            try:
                # Fetch the tickets
                url = f"{base_url}/tickets"
                params = {
                    "status": "VALID,DETAILSREQUIRED",
                    "skip": skip,
                    "top": batch_size
                }
                
                # Add a small delay to avoid rate limiting
                await asyncio.sleep(0.5)
                
                response = await client.get(url, params=params)
                
                if response.status_code != 200:
                    logger.error(f"Error response status: {response.status_code}")
                    logger.error(f"Error response body: {response.text}")
                    response.raise_for_status()
                
                ticket_data = response.json()
                tickets = ticket_data.get("rows", [])
                
                if not tickets:
                    logger.warning(f"No tickets found in batch {batch_num + 1}/{total_batches}")
                    return
                
                # Debug: Log first few tickets to see their structure
                if batch_num == 0:  # Only log for first batch to avoid spam
                    logger.debug(f"Sample tickets from API:")
                    for i, sample_ticket in enumerate(tickets[:3]):
                        ticket_name = sample_ticket.get('ticketName', 'NO_TICKET_NAME')
                        event_day = determine_ticket_event_day(ticket_name).value if ticket_name != 'NO_TICKET_NAME' else 'UNKNOWN'
                        logger.debug(f"  Ticket {i+1}: _id='{sample_ticket.get('_id', 'NO_ID')}', ticketName='{ticket_name}', eventId='{sample_ticket.get('eventId', 'NO_EVENT_ID')}', eventDay='{event_day}'")
                
                # Process the batch in a blocking transaction
                with TransactionManager(db_manager) as session:
                    # Create a GenderFixProcessor instance for this batch
                    processor = GenderFixProcessor(session, schema)
                    
                    # Process tickets in batches
                    total_tickets = 0
                    valid_athlete_tickets = 0
                    processed_tickets = 0
                    tickets_needing_update = 0
                    
                    logger.info(f"Starting to process {len(tickets)} total tickets...")
                    logger.info(f"Filtering for valid HYROX athlete tickets on {EVENT_DAY}")
                    logger.info(f"Event ID filter: {EVENT_ID}")
                    logger.info(f"Using event_processor utilities for ticket categorization and event day determination")
                    
                    for ticket in tickets:
                        total_tickets += 1
                        
                        # Apply ticket filtering first
                        if not processor.is_valid_athlete_ticket(ticket):
                            continue
                            
                        valid_athlete_tickets += 1
                        
                        # Process the ticket for gender analysis
                        analysis_data = processor.process_ticket_for_gender_analysis(ticket)
                        
                        if analysis_data:
                            processed_tickets += 1
                            
                            # Store the analysis
                            processor.store_gender_analysis(analysis_data)
                            
                            # Track tickets needing updates
                            if analysis_data['needs_update']:
                                tickets_needing_update += 1
                    
                    # Log batch completion summary
                    logger.info(
                        f"Batch {batch_num + 1}/{total_batches} complete. "
                        f"Processed: {len(tickets)} tickets, "
                        f"Valid: {valid_athlete_tickets}, "
                        f"Needs Update: {tickets_needing_update}"
                    )
                    
            except Exception as e:
                logger.error(f"Error processing batch {batch_num + 1}/{total_batches}: {str(e)}")
                raise

def ingest_gender_fix_data(token: str, event_id: str, schema: str, region: str, debug: bool = False):
    """Main gender fix ingestion function - READ ONLY, no API updates"""
    if debug:
        logger.setLevel(logging.DEBUG)
    
    db_manager = DatabaseManager(schema)
    
    try:
        # Set up the gender analysis table
        db_manager.setup_gender_analysis_table()

        # Initialize API
        api = VivenuHttpxAPI(token)
        
        # Process tickets for gender analysis
        batch_processor = BatchProcessor(batch_size=1000, max_workers=5)
        processor = batch_processor.process_tickets_for_gender_analysis(api, db_manager, schema)
        
        if processor:
            # Get tickets that need gender updates
            with TransactionManager(db_manager) as session:
                processor.session = session
                tickets_needing_update = processor.get_tickets_needing_gender_update()
                
                # Generate update payloads (for demonstration only)
                update_payloads = processor.generate_update_payloads(tickets_needing_update)
                
                # Get analysis summary
                summary = processor.get_gender_analysis_summary()
                
                # Store payloads in /data directory
                data_dir = "data"
                if not os.path.exists(data_dir):
                    os.makedirs(data_dir)
                    
                payloads_file = os.path.join(data_dir, f"gender_update_payloads_{schema}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
                with open(payloads_file, 'w') as f:
                    json.dump(update_payloads, f, indent=2)
                
                logger.info(f"Generated {len(update_payloads)} update payloads")
                logger.info(f"Payloads saved to: {payloads_file}")
                
                # Print detailed summary
                logger.info(f"Gender Analysis Summary for schema {schema}:")
                logger.info(f"  Total tickets analyzed: {summary.get('total_tickets_processed', 0)}")
                logger.info(f"  Tickets needing gender update: {summary.get('tickets_needing_update', 0)}")
                
                if summary.get('gender_breakdown'):
                    logger.info("  Gender breakdown:")
                    for gender, count in summary['gender_breakdown'].items():
                        logger.info(f"    {gender}: {count}")
                
                if summary.get('sample_tickets_needing_update'):
                    logger.info("  Sample tickets needing update:")
                    for ticket in summary['sample_tickets_needing_update'][:5]:  # Show first 5
                        logger.info(f"    {ticket['ticket_name']}: {ticket['current_gender']} → {ticket['determined_gender']}")
                
                return {
                    'payloads': update_payloads,
                    'summary': summary
                }

    except Exception as e:
        logger.error(f"Error during gender fix ingestion for schema {schema}: {str(e)}", exc_info=True)
        raise
    finally:
        # Ensure the API session is properly closed
        try:
            if api and hasattr(api, 'close'):
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
    parser.add_argument('--schema', help='Process specific schema only')
    parser.add_argument('--force-fresh', action='store_true', help='Force fresh start (drop and recreate analysis table)')
    args = parser.parse_args()
    
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
            logger.info(f"Processing schema: {config['schema']} for gender analysis")
            logger.info(f"Starting fresh analysis - previous data will be cleared")
            
            results = ingest_gender_fix_data(
                config["token"], 
                config["event_id"], 
                config["schema"], 
                config["region"],
                debug=args.debug
            )
            
            if results:
                logger.info(f"Successfully analyzed schema {config['schema']}")
                logger.info(f"  Generated {len(results['payloads'])} update payloads")
                logger.info(f"  Summary: {results['summary']}")
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