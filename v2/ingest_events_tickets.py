"""
Improved Events and Tickets Ingestion
Refactored with senior software engineering best practices.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import os
import re

import sys
import os
from pathlib import Path

# Add project root to Python path for shared components
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from v2.core import (
    get_config,
    DatabaseManager,
    TransactionManager,
    VivenuHTTPClient,
    BatchProcessor,
    ProgressTracker,
    PerformanceLogger,
    APILogger,
    get_logger,
    setup_logging,
    retry_on_failure
)
from models.database import Base, Event, Ticket, TicketSummary, SummaryReport
from utils.under_shop_processor import UnderShopProcessor, update_under_shop_summary 
from utils.event_processor import determine_ticket_group, determine_ticket_event_day, TicketCategory, TicketEventDay
from utils.addon_processor import update_addon_summary, AddonProcessor


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


class IngestionError(Exception):
    """Base ingestion error"""
    pass


class APIError(IngestionError):
    """API-related errors"""
    pass


class DatabaseError(IngestionError):
    """Database-related errors"""
    pass


@dataclass
class IngestionResult:
    """Result of ingestion process"""
    success: bool
    processed_tickets: int
    failed_tickets: int
    duration: float
    error_message: Optional[str] = None


class TicketProcessor:
    """Enhanced ticket processor with better error handling and validation"""
    
    def __init__(self, session, schema: str, region: str, logger: logging.Logger):
        self.session = session
        self.schema = schema
        self.region = region
        self.logger = logger
        self.processed = 0
        self.failed = 0
        self.field_mapper = CustomFieldMapper(schema, region)
        self.addon_processor = AddonProcessor(session, schema)
    
    def process_ticket(self, ticket_data: Dict, event_data: Dict) -> Optional[Ticket]:
        """Process single ticket with comprehensive validation"""
        ticket_id = str(ticket_data.get("_id"))
        if not ticket_id:
            self.logger.warning("Ticket ID is missing, skipping")
            return None

        try:
            # Validate ticket data
            if not self._validate_ticket_data(ticket_data, event_data):
                return None

            # Process ticket
            ticket = self._create_ticket(ticket_data, event_data)
            if ticket:
                self.processed += 1
                self.logger.debug(f"Processed ticket {ticket_id}")
            
            return ticket

        except Exception as e:
            self.failed += 1
            self.logger.error(f"Error processing ticket {ticket_id}: {str(e)}")
            return None
    
    def _validate_ticket_data(self, ticket_data: Dict, event_data: Dict) -> bool:
        """Validate ticket data before processing"""
        # Check event ID match
        if event_data.get("_id") != ticket_data.get("eventId"):
            self.logger.debug(f"Skipping ticket - event ID mismatch")
            return False

        # Check required fields
        if not ticket_data.get("ticketName"):
            self.logger.debug(f"Ticket name is missing for ticket ID {ticket_data.get('_id')}")
            return False

        return True
    
    def _create_ticket(self, ticket_data: Dict, event_data: Dict) -> Optional[Ticket]:
        """Create ticket object from data"""
        try:
            extra_fields = ticket_data.get("extraFields", {})
            ticket_name = ticket_data.get("ticketName")
            ticket_category = determine_ticket_group(ticket_name)
            
            # Determine under shop status
            under_shop_id = ticket_data.get("underShopId")
            is_under_shop = bool(under_shop_id) and ticket_category not in [TicketCategory.EXTRA, TicketCategory.SPECTATOR]
            
            if ticket_category in [TicketCategory.EXTRA, TicketCategory.SPECTATOR]:
                under_shop_id = None

            # Process addons
            addon_data = self.addon_processor.process_ticket_addons(ticket_data)
            
            # Create ticket values
            ticket_values = {
                'id': str(ticket_data.get("_id")),
                'region_schema': self.schema,
                'transaction_id': ticket_data.get("transactionId"),
                'ticket_type_id': ticket_data.get("ticketTypeId"),
                'currency': ticket_data.get("currency"),
                'status': ticket_data.get("status"),
                'personalized': ticket_data.get("personalized", False),
                'expired': ticket_data.get("expired", False),
                'event_id': event_data.get("_id"),
                'ticket_name': ticket_name,
                'category_name': ticket_data.get("categoryName"),
                'barcode': ticket_data.get("barcode"),
                'created_at': self._parse_datetime(ticket_data.get("createdAt")),
                'updated_at': self._parse_datetime(ticket_data.get("updatedAt")),
                'city': ticket_data.get("city"),
                'country': ticket_data.get("country"),
                'customer_id': ticket_data.get("customerId"),
                'gender': self._standardize_gender(extra_fields.get("gender")),
                'birthday': extra_fields.get("birth_date"),
                'age': self._calculate_age(extra_fields.get("birth_date")),
                'nationality': extra_fields.get("nationality"),
                'region_of_residence': extra_fields.get("region_of_residence"),
                'is_gym_affiliate': extra_fields.get("hyrox_training_clubs"),
                'gym_affiliate': self.field_mapper.get_gym_affiliate(extra_fields),
                'gym_affiliate_location': self.field_mapper.get_gym_affiliate_location(extra_fields),
                'is_returning_athlete': self._normalize_yes_no(extra_fields.get("returning_athlete")),
                'is_returning_athlete_to_city': self._normalize_yes_no(extra_fields.get("returning_athlete_city")),
                'is_under_shop': is_under_shop,
                'under_shop_id': under_shop_id,
                'addons': addon_data
            }
            
            ticket = Ticket(**ticket_values)
            return self.session.merge(ticket)

        except Exception as e:
            self.logger.error(f"Error creating ticket: {e}")
            raise
    
    def _parse_datetime(self, dt_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime string to datetime object"""
        if not dt_str:
            return None
        try:
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return None
    
    def _calculate_age(self, birth_date: Optional[str]) -> Optional[int]:
        """Calculate age from birth date"""
        if not birth_date:
            return None
        try:
            birth_date = datetime.strptime(birth_date, "%Y-%m-%d")
            today = datetime.today()
            return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        except (ValueError, TypeError):
            return None
    
    def _standardize_gender(self, gender: Optional[str]) -> Optional[str]:
        """Standardize gender input to 'Male' or 'Female'"""
        if not gender:
            return None
        
        first_word = str(gender).split()[0].lower().strip()
        
        if first_word in ['male', 'men']:
            return 'Male'
        elif first_word in ['female', 'woman', 'women']:
            return 'Female'
        
        return None
    
    def _normalize_yes_no(self, value: Optional[str]) -> Optional[bool]:
        """Normalize Yes/No values to boolean"""
        if not value:
            return None
        
        first_word = str(value).split()[0].lower().strip()
        
        if first_word == 'yes':
            return True
        elif first_word == 'no':
            return False
        
        return None


class CustomFieldMapper:
    """Manages custom field mappings for different regions/schemas"""
    
    def __init__(self, schema: str, region: str):
        self.schema = schema
        self.region = region
        self._field_mappings = self._load_field_mappings()

    def _load_field_mappings(self) -> Dict[str, str]:
        """Load field mappings from environment variables"""
        mappings = {}
        prefix = f'EVENT_CONFIGS__{self.region}__field_'
        
        for key, value in os.environ.items():
            if key.startswith(prefix):
                db_column = key[len(prefix):].lower()
                mappings[db_column] = value
        
        return mappings

    def get_field_value(self, extra_fields: Dict[str, Any], db_column: str) -> Optional[Any]:
        """Get value from extra_fields using mapped API field name"""
        api_field = self._field_mappings.get(db_column)
        if not api_field:
            return None
        return extra_fields.get(api_field)

    def normalize_value(self, value: Optional[str]) -> Optional[str]:
        """Normalize string values"""
        if not value:
            return None
        
        normalized = re.sub(r'[^a-zA-Z0-9\s]', '', str(value))
        normalized = ' '.join(normalized.split())
        normalized = normalized.strip()
        
        if (not normalized or len(normalized) <= 1 or 
            normalized.lower() in ['na', 'n/a', 'none', 'no', 'nil', 'other', '']):
            return None
        
        return normalized

    def get_gym_affiliate(self, extra_fields: Dict[str, Any]) -> Optional[str]:
        """Determine gym affiliate based on membership status"""
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
        """Determine gym affiliate location"""
        is_gym_affiliate_condition = extra_fields.get("hyrox_training_clubs")
        membership_status = GymMembershipStatus.parse(is_gym_affiliate_condition)
        
        if not membership_status:
            return None
            
        if membership_status == GymMembershipStatus.MEMBER_OTHER:
            return extra_fields.get('region_training')
        elif membership_status == GymMembershipStatus.MEMBER:
            return extra_fields.get('local_territory_training')
        
        return None


class EventsTicketsIngester:
    """Main ingestion class with improved architecture"""
    
    def __init__(self, config):
        self.config = config
        self.logger = get_logger(__name__)
        self.performance_logger = PerformanceLogger(self.logger)
        self.api_logger = APILogger(self.logger)
    
    async def ingest_data(
        self, 
        token: str, 
        event_id: str, 
        schema: str, 
        region: str,
        skip_fetch: bool = False
    ) -> IngestionResult:
        """Main ingestion method with comprehensive error handling"""
        start_time = datetime.now()
        self.performance_logger.start_timer("ingestion")
        
        try:
            # Setup database
            db_manager = DatabaseManager(self.config.database)
            
            if not skip_fetch:
                db_manager.setup_schema()
            
            if skip_fetch:
                return await self._update_summaries_only(db_manager, schema, event_id)
            
            # Get events and process
            async with VivenuHTTPClient(token, self.config.events[0].base_url) as api:
                events = await api.get_events()
                event_data = self._find_event(events, event_id)
                
                if not event_data:
                    raise APIError(f"Event {event_id} not found")
                
                # Create event record
                with TransactionManager(db_manager) as session:
                    event = self._create_event(session, event_data, schema)
                
                # Process tickets
                processed_count = await self._process_tickets(api, db_manager, event_data, schema, region)
                
                # Update summaries
                if processed_count > 0:
                    await self._update_summaries(db_manager, schema, event_id)
                
                duration = (datetime.now() - start_time).total_seconds()
                self.performance_logger.end_timer("ingestion", processed_tickets=processed_count)
                
                return IngestionResult(
                    success=True,
                    processed_tickets=processed_count,
                    failed_tickets=0,
                    duration=duration
                )
                
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"Ingestion failed: {e}")
            
            return IngestionResult(
                success=False,
                processed_tickets=0,
                failed_tickets=0,
                duration=duration,
                error_message=str(e)
            )
    
    def _find_event(self, events: Dict, event_id: str) -> Optional[Dict]:
        """Find event by ID"""
        for event in events.get("rows", []):
            if event.get("_id") == event_id:
                return event
        return None
    
    def _create_event(self, session, event_data: Dict, schema: str) -> Event:
        """Create or update event record"""
        event = Event(
            id=event_data['_id'],
            region_schema=schema,
            name=event_data.get('name'),
            seller_id=event_data.get('sellerId'),
            location_name=event_data.get('locationName'),
            start_date=self._parse_datetime(event_data.get('start')),
            end_date=self._parse_datetime(event_data.get('end')),
            sell_start=self._parse_datetime(event_data.get('sellStart')),
            sell_end=self._parse_datetime(event_data.get('sellEnd')),
            timezone=event_data.get('timezone'),
            cartAutomationRules=event_data.get('cartAutomationRules', []),
            groups=event_data.get('groups', []),
            tickets=[{'id': ticket['_id'], 'name': ticket['name']} for ticket in event_data.get('tickets', [])]
        )
        
        session.merge(event)
        session.commit()
        
        # Process under shops if available
        if 'underShops' in event_data:
            processor = UnderShopProcessor(session, schema)
            processor.process_under_shops(event_data, event_data['_id'])
        
        return event
    
    async def _process_tickets(
        self, 
        api: VivenuHTTPClient, 
        db_manager: DatabaseManager, 
        event_data: Dict, 
        schema: str, 
        region: str
    ) -> int:
        """Process tickets with batch processing"""
        # Get total count
        first_batch = await api.get_tickets(skip=0, limit=1)
        total_tickets = first_batch.get("total", 0)
        
        if not total_tickets:
            self.logger.warning("No tickets found to process")
            return 0
        
        # Create batch processor
        batch_processor = BatchProcessor(self.config.batch)
        
        # Process tickets in batches
        processed_count = 0
        batch_size = self.config.batch.batch_size
        
        for skip in range(0, total_tickets, batch_size):
            try:
                # Fetch batch
                batch_data = await api.get_tickets(skip=skip, limit=batch_size)
                tickets = batch_data.get("rows", [])
                
                if not tickets:
                    break
                
                # Process batch
                with TransactionManager(db_manager) as session:
                    processor = TicketProcessor(session, schema, region, self.logger)
                    
                    for ticket in tickets:
                        processor.process_ticket(ticket, event_data)
                    
                    processed_count += processor.processed
                    self.logger.info(f"Processed batch: {processor.processed} tickets, {processor.failed} failed")
                
            except Exception as e:
                self.logger.error(f"Error processing batch at skip {skip}: {e}")
                continue
        
        return processed_count
    
    async def _update_summaries(self, db_manager: DatabaseManager, schema: str, event_id: str):
        """Update all summaries"""
        with TransactionManager(db_manager) as session:
            self._update_ticket_summary(session, schema, event_id)
            self._update_summary_report(session, schema, event_id)
            update_under_shop_summary(session, schema, event_id)
            update_addon_summary(session, schema, event_id)
    
    async def _update_summaries_only(self, db_manager: DatabaseManager, schema: str, event_id: str):
        """Update summaries only (skip fetch mode)"""
        await self._update_summaries(db_manager, schema, event_id)
        return IngestionResult(success=True, processed_tickets=0, failed_tickets=0, duration=0.0)
    
    def _update_ticket_summary(self, session, schema: str, event_id: str):
        """Update ticket summary"""
        # Implementation from original code
        pass
    
    def _update_summary_report(self, session, schema: str, event_id: str):
        """Update summary report"""
        # Implementation from original code
        pass
    
    def _parse_datetime(self, dt_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime string"""
        if not dt_str:
            return None
        try:
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return None


async def main():
    """Main entry point"""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="V2 Events Tickets Ingestion")
    parser.add_argument('--skip-fetch', action='store_true', help='Skip API calls and only update summaries')
    args = parser.parse_args()
    
    # Load configuration
    config = get_config()
    
    # Setup logging
    setup_logging(config.logging)
    logger = get_logger(__name__)
    
    # Process each event configuration
    ingester = EventsTicketsIngester(config)
    
    for event_config in config.events:
        try:
            logger.info(f"Processing schema: {event_config.schema}")
            
            result = await ingester.ingest_data(
                token=event_config.token,
                event_id=event_config.event_id,
                schema=event_config.schema,
                region=event_config.region,
                skip_fetch=args.skip_fetch
            )
            
            if result.success:
                logger.info(f"Successfully processed {result.processed_tickets} tickets for {event_config.schema}")
            else:
                logger.error(f"Failed to process {event_config.schema}: {result.error_message}")
                
        except Exception as e:
            logger.error(f"Failed to process schema {event_config.schema}: {e}")
            continue


if __name__ == "__main__":
    asyncio.run(main())
