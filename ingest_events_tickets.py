import logging
import requests
import asyncio
import httpx  # Import httpx library
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text, func, inspect
from models.database import Base, Event, Ticket, TicketSummary, SummaryReport
import time
from math import ceil
from typing import Dict, Set, List, Tuple, Optional, Union, Any
from dataclasses import dataclass
from enum import Enum
import os
from dotenv import load_dotenv
import re
from utils.under_shop_processor import UnderShopProcessor, update_under_shop_summary 
from utils.event_processor import determine_ticket_group, determine_ticket_event_day, TicketCategory, TicketEventDay
from utils.addon_processor import update_addon_summary, AddonProcessor, debug_addon_storage

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
    log_filename = f'logs/ingest_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

class GymMembershipStatus(Enum):
    """Standardized gym membership status"""
    MEMBER_OTHER = "I'm a member of another"
    MEMBER = "I'm a member"
    NOT_MEMBER = "I'm not a member"
    
    @classmethod
    def parse(cls, value: Optional[str]) -> Optional['GymMembershipStatus']:
        """Parse membership status from input string"""
        if not value:
            return None
            
        normalized = value.lower().strip()
        for status in cls:
            if status.value.lower() in normalized:
                return status
        return None

class VivenuAPI:
    def __init__(self, token: str):
        self.token = token
        self.base_url = os.getenv('EVENT_API_BASE_URL', '').rstrip('/')  # Remove trailing slash if present
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        }
        logger.debug(f"API initialized with URL: {self.base_url}")
        logger.debug(f"Using headers: {self.headers}")

    def get_events(self):
        logger.debug(f"Making request to: {self.base_url}/events")
        response = requests.get(f"{self.base_url}/events", headers=self.headers, verify=False)
        response.raise_for_status()
        return response.json()

    def get_tickets(self, skip: int = 0, limit: int = 1000):
        params = {
            "status": "VALID,DETAILSREQUIRED",
            "skip": skip,
            "top": limit
        }
        logger.debug(f"Making request to: {self.base_url}/tickets with params {params}")
        response = requests.get(f"{self.base_url}/tickets", headers=self.headers, params=params, verify=False)
        response.raise_for_status()
        return response.json()

class VivenuHttpxAPI:
    """API implementation using httpx"""
    def __init__(self, token: str):
        self.token = token
        self.base_url = os.getenv('EVENT_API_BASE_URL', '').rstrip('/')  # Remove trailing slash if present
        
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
        logger.debug(f"Using headers: {self.headers}")
        
    async def _ensure_client(self):
        """Ensure httpx client exists"""
        if self._client is None:
            self._client = httpx.AsyncClient(
                headers=self.headers,
                verify=False,  # Try disabling SSL verification
                timeout=30.0
            )
        return self._client
        
    async def _get_events_async(self):
        """Async implementation of get_events using httpx"""
        client = await self._ensure_client()
        url = f"{self.base_url}/events"
        
        logger.debug(f"Making httpx request to: {url}")
        
        try:
            # Add a small delay to avoid rate limiting
            await asyncio.sleep(0.5)
            
            response = await client.get(url)
            
            if response.status_code != 200:
                logger.error(f"Error response status: {response.status_code}")
                logger.error(f"Error response body: {response.text}")
                logger.error(f"Request headers sent: {client.headers}")
                response.raise_for_status()
                
            return response.json()
        except Exception as e:
            logger.error(f"Httpx request failed: {str(e)}")
            raise
            
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
                logger.error(f"Request headers sent: {client.headers}")
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

    def get_events(self):
        """Synchronous wrapper for the async method"""
        loop = self._get_or_create_loop()
        try:
            return loop.run_until_complete(self._get_events_async())
        except RuntimeError as e:
            if str(e) == "Event loop is closed":
                # Create a new loop and try again
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                return loop.run_until_complete(self._get_events_async())
            raise

    def get_tickets(self, skip: int = 0, limit: int = 1000):
        """Synchronous wrapper for the async method"""
        loop = self._get_or_create_loop()
        try:
            return loop.run_until_complete(self._get_tickets_async(skip, limit))
        except RuntimeError as e:
            if str(e) == "Event loop is closed":
                # Create a new loop and try again
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

    def setup_schema(self):
        """Set up schema and tables"""
        with self.engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema}"))
            conn.execute(text(f"SET search_path TO {self.schema}"))
            
            # Drop existing tables
            if os.getenv('ENABLE_GROWTH_ANALYSIS', 'false').lower() != 'true':
                conn.execute(text(f"DROP TABLE IF EXISTS {self.schema}.summary_report CASCADE"))
            
            # Drop tables with both old and new names to ensure clean setup
            conn.execute(text(f"""
                -- Drop main tables
                DROP TABLE IF EXISTS {self.schema}.ticket_summary CASCADE;
                DROP TABLE IF EXISTS {self.schema}.tickets CASCADE;
                DROP TABLE IF EXISTS {self.schema}.events CASCADE;
                
                -- Drop under shop related tables
                DROP TABLE IF EXISTS {self.schema}.ticket_under_shop_summary CASCADE;
                DROP TABLE IF EXISTS {self.schema}.ticket_under_shops CASCADE;
                DROP TABLE IF EXISTS {self.schema}.ticket_volumes CASCADE;
                DROP TABLE IF EXISTS {self.schema}.ticket_volume CASCADE;
                
                -- Drop addon tables
                DROP TABLE IF EXISTS {self.schema}.ticket_addon_summary CASCADE;
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

def verify_tables(session, schema: str):
    """Verify that tables exist with correct columns"""
    try:
        # Check if event_id column exists
        result = session.execute(text(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = '{schema}' 
            AND table_name = 'tickets' 
            AND column_name = 'event_id'
        """))
        
        if not result.fetchone():
            logger.error(f"event_id column not found in {schema}.tickets")
            raise Exception("Required columns not found")
            
        logger.info(f"Table verification successful for schema {schema}")
    except Exception as e:
        logger.error(f"Table verification failed for schema {schema}: {e}")
        raise

# Add logging configuration
class LogConfig:
    DEBUG_ENABLED = False  # Toggle for debug logging

    @classmethod
    def set_debug(cls, enabled: bool):
        cls.DEBUG_ENABLED = enabled
        if enabled:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

def calculate_age(birth_date) -> Union[int, None]:
    if birth_date:
        birth_date = datetime.strptime(birth_date, "%Y-%m-%d")
        today = datetime.today()
        return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
    return None

def standardize_gender(gender: str) -> Union[str, None]:
    """Standardize gender input to 'Male' or 'Female'.
    
    Handles:
    1. Single gender strings: 'male', 'men', 'female', 'woman', 'women'
    2. Multi-language gender strings where English appears first:
       - "Female เพศหญิง" (Thai)
       - "Male 남성" (Korean)
       - "Female 女性" (Japanese/Chinese)
    """
    if not gender:
        return None
        
    # Get first word and normalize
    first_word = str(gender).split()[0].lower().strip()
    
    if first_word in ['male', 'men']:
        return 'Male'
    elif first_word in ['female', 'woman', 'women']:
        return 'Female'
    
    return None

def parse_datetime(dt_str):
    """Parse datetime string to datetime object"""
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        return None

def update_ticket_summary(session, schema: str, event_id: str):
    """Update ticket type summary for an event"""
    try:
        # Get event details first
        event = session.query(Event).filter(Event.id == event_id).first()
        if not event:
            logger.warning(f"Event {event_id} not found for summary update")
            return

        # Create a lookup dictionary for ticket names using ticket_type_id
        ticket_name_map = {ticket.get('id'): ticket.get('name') for ticket in event.tickets}
        
        # Get ticket counts grouped by type and category
        ticket_counts = (
            session.query(
                Ticket.event_id,
                Ticket.ticket_type_id,
                func.count().label('total_count')
            )
            .filter(Ticket.event_id == event_id)
            .group_by(
                Ticket.event_id,
                Ticket.ticket_type_id
            )
            .all()
        )

        # Update summary records
        for count in ticket_counts:
            ticket_name = ticket_name_map.get(count.ticket_type_id, '')
            summary_id = f"{count.event_id}_{count.ticket_type_id}"
            
            summary = session.get(TicketSummary, summary_id)
            if summary:
                summary.total_count = count.total_count
            else:
                summary = TicketSummary(
                    id=summary_id,
                    event_id=count.event_id,
                    event_name=event.name,
                    ticket_type_id=count.ticket_type_id,
                    ticket_name=ticket_name,
                    ticket_category=determine_ticket_group(ticket_name).value,
                    ticket_event_day=determine_ticket_event_day(ticket_name).value,
                    total_count=count.total_count
                )
                session.add(summary)

        session.commit()
        

        logger.info(f"Updated ticket summary for event: {event_id} in schema: {schema}")

    except Exception as e:
        session.rollback()
        logger.error(f"Error updating ticket summary in schema {schema}: {e}")
        raise

def get_ticket_summary(session, schema: str, event_id: str) -> Dict[str, SummaryReport]:
    """Get the summarized ticket counts for the event."""
    try:
        # Query to get the total counts for each ticket group and collect ticket info
        with open('sql/get_summary_report.sql', 'r') as file:
            sql_template = file.read()
        query = text(sql_template.replace('{SCHEMA}', schema))
        results = session.execute(query, {"event_id": event_id}).fetchall()
        
        # Convert results to a dictionary with additional info
        summary_data = {}
        for row in results:
            summary_data[row[0]] = {
                'total_count': row[3],
                'ticket_type_ids': row[1],
                'ticket_names': row[2]
            }
        
        return summary_data

    except Exception as e:
        logger.error(f"Error getting ticket summary: {e}")
        return {}

def update_summary_report(session, schema: str, event_id: str):
    """Update summary report with current ticket counts while preserving history"""
    try:
        logger.info(f"Updating summary report for event {event_id} in schema {schema}")
        current_time = datetime.now()
        summary_data = get_ticket_summary(session, schema, event_id)
        
        for ticket_group, data in summary_data.items():
            logger.info(f"Inserting summary for ticket group: {ticket_group}")
            summary = SummaryReport(
                event_id=event_id,
                ticket_group=ticket_group,
                total_count=data.get('total_count', 0),
                ticket_type_ids=data.get('ticket_type_ids'),
                ticket_names=data.get('ticket_names'),
                created_at=current_time,
                updated_at=current_time
            )
            session.add(summary)
        
        session.commit()
        logger.info(f"Summary report updated for event {event_id}")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error updating summary report: {e}")
        raise

def create_event(session: sessionmaker, event_data: dict, schema: str) -> Event:
    """Create or update event record"""
    event = Event(
        id=event_data['_id'],
        region_schema=schema,
        name=event_data.get('name'),
        seller_id=event_data.get('sellerId'),
        location_name=event_data.get('locationName'),
        start_date=parse_datetime(event_data.get('start')),
        end_date=parse_datetime(event_data.get('end')),
        sell_start=parse_datetime(event_data.get('sellStart')),
        sell_end=parse_datetime(event_data.get('sellEnd')),
        timezone=event_data.get('timezone'),
        cartAutomationRules=event_data.get('cartAutomationRules', []),
        groups=event_data.get('groups', []),
        tickets=[{'id': ticket['_id'], 'name': ticket['name']} for ticket in event_data.get('tickets', [])]
    )
    
    try:
        session.merge(event)
        session.commit()
        
        # Process underShops if available
        if 'underShops' in event_data:
            processor = UnderShopProcessor(session, schema)
            processor.process_under_shops(event_data, event_data['_id'])
            
        return event
    except Exception as e:
        session.rollback()
        logger.error(f"Error creating event: {e}")
        raise

class CustomFieldMapper:
    """Manages custom field mappings for different regions/schemas"""
    
    def __init__(self, schema: str, region: str):
        self.schema = schema
        self.region = region
        self._field_mappings = self._load_field_mappings()

    def _load_field_mappings(self) -> Dict[str, str]:
        """
        Dynamically load all field mappings from environment variables
        Format: EVENT_CONFIGS__{region}__field_{database_column}={api_field_name}
        """
        mappings = {}
        prefix = f'EVENT_CONFIGS__{self.region}__field_'
        
        # Scan all environment variables for field mappings
        for key, value in os.environ.items():
            if key.startswith(prefix):
                # Convert environment key to database column name
                db_column = key[len(prefix):].lower()
                mappings[db_column] = value
        
        return mappings

    def get_field_value(self, extra_fields: Dict[str, Any], db_column: str) -> Optional[Any]:
        """Get value from extra_fields using the mapped API field name"""
        api_field = self._field_mappings.get(db_column)
        if not api_field:
            return None
        value = extra_fields.get(api_field)
        return value

    def normalize_value(self, value: Optional[str]) -> Optional[str]:
        """
        Normalize string values by trimming whitespace, removing special characters,
        and handling invalid/empty/NA variations.
        
        Args:
            value: Input string to normalize
            
        Returns:
            Normalized string or None if value is empty/invalid/NA
        """
        if not value:
            return None
        
        # Remove special characters and extra spaces
        normalized = re.sub(r'[^a-zA-Z0-9\s]', '', str(value))
        normalized = ' '.join(normalized.split())  # Handle multiple spaces
        normalized = normalized.strip()
        
        # Return None for empty, single character, or invalid values
        if (not normalized or                          # Empty string
            len(normalized) <= 1 or                    # Single character
            normalized.lower() in [                    # Invalid values
                'na', 'n/a', 'none', 'no', 
                'nil', 'other', ''
            ]):
            return None
        
        return normalized

    def get_gym_affiliate(self, extra_fields: Dict[str, Any]) -> Optional[str]:
        """
        Determine gym affiliate based on membership status and region.
        
        Args:
            extra_fields: Dictionary containing ticket extra fields
            
        Returns:
            Normalized gym affiliate value or None
        """
        is_gym_affiliate_condition = extra_fields.get("hyrox_training_clubs")
        membership_status = GymMembershipStatus.parse(is_gym_affiliate_condition)
        
        if not membership_status:
            return None
        
        if membership_status == GymMembershipStatus.MEMBER_OTHER:
            return self.normalize_value(extra_fields.get('hyrox_training_club_other_territory_name'))
        elif membership_status == GymMembershipStatus.MEMBER:
            return self.normalize_value(extra_fields.get('local_territory_training_club'))
        
        return self.normalize_value(extra_fields.get('gym_club_community'))

    def get_gym_affiliate_location(self, extra_fields: Dict[str, Any]) -> Optional[str]:
        """
        Determine gym affiliate location based on membership status
        
        Args:
            extra_fields: Dictionary containing ticket extra fields
            
        Returns:
            Resolved gym affiliate location value or None
        """
        # Get membership status
        is_gym_affiliate_condition = extra_fields.get("hyrox_training_clubs")
        membership_status = GymMembershipStatus.parse(is_gym_affiliate_condition)
        
        if not membership_status:
            return None
            
        if membership_status == GymMembershipStatus.MEMBER_OTHER:
            return extra_fields.get('region_training')
        elif membership_status == GymMembershipStatus.MEMBER:
            return extra_fields.get('local_territory_training')
        
        return None

class TicketProcessor:
    """Efficient ticket processing with lookup caching and validation"""
    
    def __init__(self, session, schema: str, region: str):
        self.session = session
        self.schema = schema
        self.region = region
        self.processed = 0
        self.failed = 0
        self.existing_tickets_cache = {}
        self.field_mapper = CustomFieldMapper(schema, region)
        self.addon_processor = AddonProcessor(session, schema)
        self.force_addon_update = True  # Force update addon data

    def get_existing_ticket(self, ticket_id: str) -> Optional[Ticket]:
        """Efficiently lookup ticket from cache or database"""
        cache_key = f"{ticket_id}_{self.schema}"
        if cache_key not in self.existing_tickets_cache:
            ticket = self.session.query(Ticket).filter(
                Ticket.id == ticket_id,
                Ticket.region_schema == self.schema
            ).first()
            self.existing_tickets_cache[cache_key] = ticket
        return self.existing_tickets_cache[cache_key]
    
    def process_ticket(self, ticket_data: Dict, event_data: Dict) -> Optional[Ticket]:
        """Process single ticket with validation and efficient lookup"""
        ticket_id = str(ticket_data.get("_id"))
        if not ticket_id:
            logger.warning("Ticket ID is missing, skipping")
            return None

        try:
            # Basic validations
            event_id = event_data.get("_id")
            if event_id != ticket_data.get("eventId"):
                logger.debug(f"Skipping ticket {ticket_id} - event ID mismatch")
                return None

            ticket_name = ticket_data.get("ticketName")
            if not ticket_name:
                logger.debug(f"Ticket name is missing for ticket ID {ticket_id}")
                return None

            # Get extra fields
            extra_fields = ticket_data.get("extraFields", {})
            
            # Determine ticket category
            ticket_category = determine_ticket_group(ticket_name)
            
            # Check if the ticket was purchased through an under shop
            # Only set is_under_shop=True if the ticket is not EXTRA or SPECTATOR
            under_shop_id = ticket_data.get("underShopId")
            is_under_shop = bool(under_shop_id) and ticket_category not in [TicketCategory.EXTRA, TicketCategory.SPECTATOR]
            
            # If category is EXTRA or SPECTATOR, we don't track under_shop_id
            if ticket_category in [TicketCategory.EXTRA, TicketCategory.SPECTATOR]:
                under_shop_id = None

            # Process addOns - simplified to just get the name
            addon_data = self.addon_processor.process_ticket_addons(ticket_data, event_id)
            
            # Debug: log the raw addOns data
            raw_addons = ticket_data.get('addOns', [])
            if raw_addons:
                logger.debug(f"Ticket {ticket_id} raw addOns: {raw_addons}")
                logger.debug(f"Ticket {ticket_id} processed addon: {addon_data}")
            
            # Check if ticket exists
            existing_ticket = self.get_existing_ticket(ticket_id)
            if existing_ticket:
                # Always update addon data for existing tickets
                if self.force_addon_update or existing_ticket.addons != addon_data:
                    existing_ticket.addons = addon_data
                    logger.debug(f"Updated existing ticket {ticket_id} addon: {addon_data}")
                    self.processed += 1  # Count as processed since we updated it
                return existing_ticket
            else:
                # Create new ticket
                ticket_values = {
                    'id': ticket_id,
                    'region_schema': self.schema,
                    'transaction_id': ticket_data.get("transactionId"),
                    'ticket_type_id': ticket_data.get("ticketTypeId"),
                    'currency': ticket_data.get("currency"),
                    'status': ticket_data.get("status"),
                    'personalized': ticket_data.get("personalized", False),
                    'expired': ticket_data.get("expired", False),
                    'event_id': event_id,
                    'ticket_name': ticket_name,
                    'category_name': ticket_data.get("categoryName"),
                    'barcode': ticket_data.get("barcode"),
                    'created_at': parse_datetime(ticket_data.get("createdAt")),
                    'updated_at': parse_datetime(ticket_data.get("updatedAt")),
                    'city': ticket_data.get("city"),
                    'country': ticket_data.get("country"),
                    'customer_id': ticket_data.get("customerId"),
                    'gender': standardize_gender(extra_fields.get("gender")),
                    'birthday': extra_fields.get("birth_date"),
                    'age': calculate_age(extra_fields.get("birth_date")),
                    'nationality': extra_fields.get("nationality"),
                    'region_of_residence': extra_fields.get("region_of_residence"),
                    'is_gym_affiliate': extra_fields.get("hyrox_training_clubs"),
                    'gym_affiliate': self.field_mapper.get_gym_affiliate(extra_fields),
                    'gym_affiliate_location': self.field_mapper.get_gym_affiliate_location(extra_fields),
                    'is_returning_athlete': normalize_yes_no(extra_fields.get("returning_athlete")),
                    'is_returning_athlete_to_city': normalize_yes_no(extra_fields.get("returning_athlete_city")),
                    'is_under_shop': is_under_shop,
                    'under_shop_id': under_shop_id,
                    'addons': addon_data  # Now just a string or None
                }
                new_ticket = Ticket(**ticket_values)
                self.session.add(new_ticket)
                self.existing_tickets_cache[f"{ticket_id}_{self.schema}"] = new_ticket
                logger.debug(f"Created new ticket {ticket_id} with addon: {addon_data}")
                self.processed += 1
                return new_ticket

        except Exception as e:
            logger.error(f"Error processing ticket {ticket_id}: {str(e)}")
            self.failed += 1
            return None

    def clear_cache(self):
        """Clear the lookup cache"""
        self.existing_tickets_cache.clear()

def process_batch(session, tickets: List, event_data: Dict, schema: str, region: str):
    """Process batch of tickets using TicketProcessor"""
    processed = 0
    failed = 0
    
    processor = TicketProcessor(session, schema, region)
    
    for ticket in tickets:
        try:
            result = processor.process_ticket(ticket, event_data)
            if result:
                processed += 1
        except Exception as e:
            failed += 1
            logger.error(f"Failed to process ticket {ticket.get('_id')}: {str(e)}")
            continue
    
    processor.clear_cache()
    logger.info(f"Batch summary - Processed: {processed}, Failed: {failed}")
    return processed

class BatchProcessor:
    def __init__(self, batch_size: int = 1000, max_workers: int = 5):
        self.batch_size = batch_size
        self.max_workers = max_workers

    def process_tickets(self, api, db_manager: DatabaseManager, event_data: Dict, schema: str, region: str) -> int:
        """Process tickets in optimized batches"""
        try:
            # Get the first batch to determine total count
            first_batch = api.get_tickets(skip=0, limit=1)
            total_tickets = first_batch.get("total", 0)
            
            if not total_tickets:
                logger.warning("No tickets found to process")
                return 0

            total_batches = ceil(total_tickets / self.batch_size)
            processed_total = 0
            logger.info(f"Processing {total_tickets} tickets in {total_batches} batches")

            # Process in chunks to control parallelism
            for chunk_start in range(0, total_batches, self.max_workers):
                chunk_end = min(chunk_start + self.max_workers, total_batches)
                
                # For httpx API which is async capable
                if isinstance(api, VivenuHttpxAPI):
                    # Process httpx API batches with ThreadPoolExecutor instead of asyncio
                    # This avoids event loop issues
                    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        futures = []
                        for batch_num in range(chunk_start, chunk_end):
                            skip = batch_num * self.batch_size
                            futures.append(
                                executor.submit(
                                    self._process_httpx_batch, 
                                    api.token, 
                                    api.base_url,
                                    api.headers,
                                    db_manager, 
                                    event_data, 
                                    schema, 
                                    region, 
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
                else:
                    # For non-async APIs (like regular VivenuAPI), process sequentially
                    for batch_num in range(chunk_start, chunk_end):
                        try:
                            skip = batch_num * self.batch_size
                            # Fetch batch
                            response = api.get_tickets(skip=skip, limit=self.batch_size)
                            tickets = response.get("rows", [])
                            
                            if tickets:
                                # Process batch in a transaction
                                with TransactionManager(db_manager) as session:
                                    processed = process_batch(session, tickets, event_data, schema, region)
                                    processed_total += processed
                                    logger.info(
                                        f"Batch {batch_num + 1}/{total_batches} complete. "
                                        f"Processed: {processed}/{len(tickets)} tickets."
                                    )
                        except Exception as e:
                            logger.error(f"Error processing batch {batch_num + 1}/{total_batches}: {str(e)}")

            return processed_total
            
        except Exception as e:
            logger.error(f"Error in process_tickets: {str(e)}")
            raise
            
    def _process_httpx_batch(self, token, base_url, headers, db_manager, event_data, schema, region, batch_num, skip, total_batches, batch_size):
        """Process a single batch using httpx in its own thread and event loop"""
        try:
            # Create a new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                # Run the async processing
                return loop.run_until_complete(
                    self._process_single_batch(token, base_url, headers, db_manager, event_data, schema, region, batch_num, skip, total_batches, batch_size)
                )
            finally:
                # Always clean up the event loop
                loop.close()
                
        except Exception as e:
            logger.error(f"Error processing batch {batch_num + 1}/{total_batches}: {str(e)}")
            raise

    async def _process_single_batch(self, token, base_url, headers, db_manager, event_data, schema, region, batch_num, skip, total_batches, batch_size):
        """Process a single batch with a fresh httpx client"""
        # Create a new httpx client for this batch only
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
                    return 0
                
                # Process the batch in a blocking transaction
                with TransactionManager(db_manager) as session:
                    processed = process_batch(session, tickets, event_data, schema, region)
                    logger.info(
                        f"Batch {batch_num + 1}/{total_batches} complete. "
                        f"Processed: {processed}/{len(tickets)} tickets."
                    )
                    return processed
                    
            except Exception as e:
                logger.error(f"Error processing batch {batch_num + 1}/{total_batches}: {str(e)}")
                raise

def ingest_data(token: str, event_id: str, schema: str, region: str, skip_fetch: bool = False, debug: bool = False):
    """Main ingestion function"""
    LogConfig.set_debug(debug)
    db_manager = DatabaseManager(schema)
    
    try:
        if not skip_fetch:
            db_manager.setup_schema()

        if skip_fetch:
            with TransactionManager(db_manager) as session:
                update_ticket_summary(session, schema, event_id)
                update_summary_report(session, schema, event_id)
                update_under_shop_summary(session, schema, event_id)
                update_addon_summary(session, schema, event_id)
            return

        # Initialize variables
        api = None
        events = None
        api_type = None
        
        # Start with httpx implementation since it's working
        try:
            logger.info("Using httpx implementation for API access")
            api = VivenuHttpxAPI(token)
            events = api.get_events()
            logger.info("Successfully connected with httpx implementation")
            api_type = "httpx"
        except Exception as e:
            logger.warning(f"httpx implementation failed: {str(e)}")
            # Clean up httpx client if needed
            if api and hasattr(api, 'close') and api_type == "httpx":
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(api.close())
                except Exception:
                    pass
            
            # Fall back to standard API as last resort
            try:
                logger.info("Falling back to standard requests implementation")
                api = VivenuAPI(token)
                events = api.get_events()
                logger.info("Successfully connected with standard requests implementation")
                api_type = "requests"
            except Exception as e:
                logger.error(f"All API implementations failed. Last error: {str(e)}")
                raise
        
        if not events or not api:
            logger.error("Failed to get events from API")
            return
            
        found_event_data = None
        
        with TransactionManager(db_manager) as session:
            verify_tables(session, schema)
            for event_data in events["rows"]:
                if event_data.get("_id") == event_id:
                    event = create_event(session, event_data, schema)
                    found_event_data = event_data
                    logger.info(f"Found matching event: {event_id}")
                    break

        if not found_event_data:
            logger.error(f"Event {event_id} not found")
            return

        # Process tickets with optimized batching
        batch_processor = BatchProcessor(batch_size=1000, max_workers=5)
        processed_count = batch_processor.process_tickets(api, db_manager, found_event_data, schema, region)
        
        if processed_count > 0:
            # Debug addon storage before updating summaries
            with TransactionManager(db_manager) as session:
                debug_addon_storage(session, schema, event_id)
            
            # Update summaries in final transaction
            with TransactionManager(db_manager) as session:
                update_ticket_summary(session, schema, event_id)
                update_summary_report(session, schema, event_id)
                update_under_shop_summary(session, schema, event_id)
                update_addon_summary(session, schema, event_id)
                
            logger.info(f"Successfully processed {processed_count} tickets for event {event_id}")

    except Exception as e:
        logger.error(f"Error during ingestion for schema {schema}: {str(e)}", exc_info=True)
        raise
    finally:
        # Ensure the API session is properly closed
        try:
            if api and hasattr(api, 'close') and api_type == "httpx":
                try:
                    # Create a new event loop for cleanup to avoid "loop is closed" errors
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

def normalize_yes_no(value: Optional[str]) -> Optional[bool]:
    """Normalize Yes/No values to boolean, handling any language
    
    Args:
        value: Input string that starts with Yes/No followed by optional translation
        
    Returns:
        bool: True for strings starting with "Yes", False for strings starting with "No", 
              None for invalid/empty values
    """
    if not value:
        return None
        
    # Get first word (always English Yes/No)
    first_word = str(value).split()[0].lower().strip()
    
    if first_word == 'yes':
        return True
    elif first_word == 'no':
        return False
    
    return None

if __name__ == "__main__":
    load_dotenv()
    
    # Add command line argument for debug mode
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--skip_fetch', action='store_true', help='Enable skipping API calls')
    parser.add_argument('--migrate', action='store_true', help='Run database migration for table renames')
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
            logger.info(f"Processing schema: {config['schema']}")
            ingest_data(
                config["token"], 
                config["event_id"], 
                config["schema"], 
                config["region"],
                skip_fetch=args.skip_fetch,
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