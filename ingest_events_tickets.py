import logging
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text, func, inspect
from models.database import Base, Event, Ticket, TicketTypeSummary, SummaryReport
import time
from math import ceil
from typing import Dict, Set, List, Tuple, Optional, Union, Any
from dataclasses import dataclass
from enum import Enum
from collections import defaultdict
import os
from dotenv import load_dotenv

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

class TicketCategory(Enum):
    SINGLE = "single"
    DOUBLES = "double"
    RELAY = "relay"
    SPECTATOR = "spectator"
    EXTRA = "extra"
    
class TicketEventDay(Enum):
    FRIDAY = "friday"
    SATURDAY = "saturday"
    SUNDAY = "sunday"

class GymMembershipStatus(Enum):
    """Standardized gym membership status"""
    MEMBER = "I'm a member"
    MEMBER_OTHER = "I'm a member of another"
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
        self.base_url = os.getenv('EVENT_API_BASE_URL', '')  # Use env var with fallback
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        }

    def get_events(self):
        response = requests.get(f"{self.base_url}/events", headers=self.headers)
        response.raise_for_status()
        return response.json()

    def get_tickets(self, skip: int = 0, limit: int = 1000):
        params = {
            "status": "VALID,DETAILSREQUIRED",
            "skip": skip,
            "top": limit
        }
        response = requests.get(f"{self.base_url}/tickets", headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

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
            conn.execute(text(f"""
                DROP TABLE IF EXISTS {self.schema}.ticket_type_summary CASCADE;
                DROP TABLE IF EXISTS {self.schema}.tickets CASCADE;
                DROP TABLE IF EXISTS {self.schema}.events CASCADE;
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

def determine_ticket_group(ticket_name: str) -> TicketCategory:
    """Determine basic ticket group (single, double, relay, spectator, extra)"""
    name_lower = ticket_name.lower()
    if 'friend' in name_lower or 'sportograf' in name_lower or 'transfer' in name_lower or 'complimentary' in name_lower:
        return TicketCategory.EXTRA 
    elif 'double' in name_lower:
        return TicketCategory.DOUBLES
    elif 'relay' in name_lower:
        return TicketCategory.RELAY
    elif 'spectator' in name_lower:
        return TicketCategory.SPECTATOR
    return TicketCategory.SINGLE

def determine_ticket_event_day(ticket_name: str) -> TicketEventDay:
    """Determine basic ticket group (friday, saturday, sunday)"""
    name_lower = ticket_name.lower()
    if 'sunday' in name_lower:
        return TicketEventDay.SUNDAY 
    elif 'saturday' in name_lower:
        return TicketEventDay.SATURDAY
    elif 'friday' in name_lower:
        return TicketEventDay.FRIDAY
    return TicketEventDay.SATURDAY

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
    """Standardize gender input to 'Male' or 'Female'."""
    if gender:
        gender_lower = gender.lower()
        if gender_lower in ['male', 'men']:
            return 'Male'
        elif gender_lower in ['female', 'woman', 'women']:
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
            
            summary = session.get(TicketTypeSummary, summary_id)
            if summary:
                summary.total_count = count.total_count
            else:
                summary = TicketTypeSummary(
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
        return event
    except Exception as e:
        session.rollback()
        logger.error(f"Error creating event: {e}")
        raise

class CustomFieldMapper:
    """Manages custom field mappings for different regions/schemas"""
    
    def __init__(self, schema: str):
        self.schema = schema
        self._field_mappings = self._load_field_mappings()

    def _load_field_mappings(self) -> Dict[str, str]:
        """
        Dynamically load all field mappings from environment variables
        Format: EVENT_CONFIGS__{schema}__field_{database_column}={api_field_name}
        """
        mappings = {}
        prefix = f'EVENT_CONFIGS__{self.schema}__field_'
        
        # Scan all environment variables for field mappings
        for key, value in os.environ.items():
            if key.startswith(prefix):
                # Convert environment key to database column name
                db_column = key[len(prefix):].lower()
                mappings[db_column] = value
                
        logger.debug(f"Loaded field mappings for schema {self.schema}: {mappings}")
        return mappings

    def get_field_value(self, extra_fields: Dict[str, Any], db_column: str) -> Optional[Any]:
        """Get value from extra_fields using the mapped API field name"""
        api_field = self._field_mappings.get(db_column)
        if not api_field:
            return None
        return extra_fields.get(api_field)

    def get_gym_affiliate(self, extra_fields: Dict[str, Any]) -> Optional[str]:
        """
        Determine gym affiliate based on membership status and region
        
        Args:
            extra_fields: Dictionary containing ticket extra fields
            
        Returns:
            Resolved gym affiliate value or None
        """
        # Get membership status
        membership_raw = self.get_field_value(extra_fields, 'is_gym_affiliate')
        membership_status = GymMembershipStatus.parse(membership_raw)
        
        if not membership_status:
            return None
            
        if membership_status == GymMembershipStatus.MEMBER_OTHER:
            print('hey : ', membership_raw, ' -- ', self.get_field_value(extra_fields, 'gym_affiliate_other_country'))
            return self.get_field_value(extra_fields, 'gym_affiliate_other_country')
        elif membership_status == GymMembershipStatus.MEMBER:
            print('hey : ', membership_raw, ' -- ', self.get_field_value(extra_fields, 'gym_affiliate'))
            return self.get_field_value(extra_fields, 'gym_affiliate')
        
        return None

class TicketProcessor:
    """Efficient ticket processing with lookup caching and validation"""
    
    def __init__(self, session, schema):
        self.session = session
        self.schema = schema
        self.existing_tickets_cache = {}
        self.field_mapper = CustomFieldMapper(schema)
    
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
                logger.warning(f"Ticket name is missing for ticket ID {ticket_id}")
                return None

            # Get extra fields
            extra_fields = ticket_data.get("extraFields", {})

            # Get membership status for logging
            membership_raw = self.field_mapper.get_field_value(extra_fields, 'is_gym_affiliate')
            membership_status = GymMembershipStatus.parse(membership_raw)
            if membership_status:
                logger.debug(f"Ticket {ticket_id} membership status: {membership_status.name}")

            # Prepare ticket values
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
                'created_at': ticket_data.get("createdAt"),
                'updated_at': ticket_data.get("updatedAt"),
                'city': ticket_data.get("city"),
                'country': ticket_data.get("country"),
                'customer_id': ticket_data.get("customerId"),
                'gender': standardize_gender(extra_fields.get("gender")),
                'birthday': extra_fields.get("birth_date"),
                'age': calculate_age(extra_fields.get("birth_date")),
                'region_of_residence': self.field_mapper.get_field_value(extra_fields, 'region_of_residence'),
                'is_gym_affiliate': membership_raw,
                'gym_affiliate': self.field_mapper.get_gym_affiliate(extra_fields)
            }

            # Update or create ticket
            existing_ticket = self.get_existing_ticket(ticket_id)
            if existing_ticket:
                for key, value in ticket_values.items():
                    setattr(existing_ticket, key, value)
                return existing_ticket
            else:
                new_ticket = Ticket(**ticket_values)
                self.session.add(new_ticket)
                self.existing_tickets_cache[f"{ticket_id}_{self.schema}"] = new_ticket
                return new_ticket

        except Exception as e:
            logger.error(f"Error processing ticket {ticket_id}: {str(e)}")
            return None

    def clear_cache(self):
        """Clear the lookup cache"""
        self.existing_tickets_cache.clear()

def process_batch(session, tickets: List, event_data: Dict, schema: str):
    """Process batch of tickets using TicketProcessor"""
    processed = 0
    failed = 0
    
    processor = TicketProcessor(session, schema)
    
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

    def process_tickets(self, api: VivenuAPI, db_manager: DatabaseManager, event_data: Dict, schema: str) -> int:
        """Process tickets in optimized batches with parallel API requests"""
        first_batch = api.get_tickets(skip=0, limit=1)
        total_tickets = first_batch.get("total", 0)
        if not total_tickets:
            logger.warning("No tickets found to process")
            return 0

        total_batches = ceil(total_tickets / self.batch_size)
        processed_total = 0
        logger.info(f"Processing {total_tickets} tickets in {total_batches} batches")

        # Process in chunks of max_workers for controlled parallelism
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for chunk_start in range(0, total_batches, self.max_workers):
                chunk_end = min(chunk_start + self.max_workers, total_batches)
                
                # Submit API requests in parallel
                future_to_batch = {
                    executor.submit(self._fetch_batch, api, batch_num): batch_num
                    for batch_num in range(chunk_start, chunk_end)
                }

                # Process results as they complete
                for future in as_completed(future_to_batch):
                    batch_num = future_to_batch[future]
                    try:
                        tickets = future.result()
                        if tickets:
                            # Process batch in its own transaction
                            with TransactionManager(db_manager) as session:
                                processed = process_batch(session, tickets, event_data, schema)
                                processed_total += processed
                                logger.info(
                                    f"Batch {batch_num + 1}/{total_batches} complete. "
                                    f"Processed: {processed}/{len(tickets)} tickets. "
                                )
                    except Exception as e:
                        logger.error(f"Error processing batch {batch_num}: {str(e)}")

        return processed_total

    def _fetch_batch(self, api: VivenuAPI, batch_num: int) -> List[Dict]:
        """Fetch a single batch of tickets with retries"""
        max_retries = 3
        retry_delay = 1  # seconds

        for attempt in range(max_retries):
            try:
                skip = batch_num * self.batch_size
                response = api.get_tickets(skip=skip, limit=self.batch_size)
                return response.get("rows", [])  # Return empty list as default
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed to fetch batch {batch_num} after {max_retries} attempts: {str(e)}")
                    return []  # Return empty list on failure
                time.sleep(retry_delay * (attempt + 1))

def ingest_data(token: str, event_id: str, schema: str, skip_fetch: bool = False, debug: bool = False):
    """Main ingestion function"""
    LogConfig.set_debug(debug)
    api = VivenuAPI(token)
    db_manager = DatabaseManager(schema)
    
    try:
        if not skip_fetch:
            db_manager.setup_schema()

        if skip_fetch:
            with TransactionManager(db_manager) as session:
                update_ticket_summary(session, schema, event_id)
                update_summary_report(session, schema, event_id)
            return

        # Process event data
        events = api.get_events()
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
        processed_count = batch_processor.process_tickets(api, db_manager, found_event_data, schema)
        
        if processed_count > 0:
            # Update summaries in final transaction
            with TransactionManager(db_manager) as session:
                update_ticket_summary(session, schema, event_id)
                update_summary_report(session, schema, event_id)
                
            logger.info(f"Successfully processed {processed_count} tickets for event {event_id}")

    except Exception as e:
        logger.error(f"Error during ingestion for schema {schema}: {str(e)}", exc_info=True)
        raise

def get_event_configs():
    """Get all event configurations from environment"""
    from collections import defaultdict
    
    configs = defaultdict(dict)
    for key, value in os.environ.items():
        if key.startswith("EVENT_CONFIGS__"):
            _, region, param = key.split("__", 2)
            if param in ["token", "event_id", "schema_name"]:
                configs[region][param] = value
    
    return [
        {
            "token": config["token"],
            "event_id": config["event_id"],
            "schema": config["schema_name"]
        }
        for config in configs.values()
        if all(k in config for k in ["token", "event_id", "schema_name"])
    ]

if __name__ == "__main__":
    load_dotenv()
    
    # Add command line argument for debug mode
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--skip_fetch', action='store_true', help='Enable skipping API calls')
    args = parser.parse_args()
    
    configs = get_event_configs()
    if not configs:
        raise ValueError("No valid event configurations found in environment")
    
    # Process each config
    for config in configs:
        try:
            logger.info(f"Processing schema: {config['schema']}")
            ingest_data(
                config["token"], 
                config["event_id"], 
                config["schema"], 
                skip_fetch=args.skip_fetch,
                debug=args.debug
            )
        except Exception as e:
            logger.error(f"Failed to process schema {config['schema']}: {e}")
            continue 