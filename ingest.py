import logging
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text, func
from models.database import Base, Event, Ticket, TicketTypeSummary, SummaryReport
import time
from math import ceil
from typing import Dict, Set, List, Tuple, Optional
from dataclasses import dataclass
from enum import Enum
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TicketCategory(Enum):
    SINGLE = "Single"
    DOUBLES = "Doubles"
    RELAY = "Relay"
    SPECTATOR = "Spectator"

@dataclass
class GroupTickets:
    """Data class to hold group information and its tickets"""
    group_id: str
    group_name: str
    tickets: List[str]

@dataclass
class AutomationRule:
    """Data class to represent a cart automation rule"""
    trigger_group: GroupTickets
    target_groups: List[GroupTickets]

class TicketGrouper:
    """Class to handle ticket grouping logic"""
    def __init__(self, event_groups: List[dict]):
        self.event_groups = event_groups
        self._group_lookup = {
            group['_id']: group 
            for group in event_groups
        }

    def find_group(self, group_id: str) -> Optional[dict]:
        """Find a group by its ID"""
        return self._group_lookup.get(group_id)

    def create_group_tickets(self, group_id: str) -> Optional[GroupTickets]:
        """Create a GroupTickets instance from a group ID"""
        group = self.find_group(group_id)
        if not group:
            return None
        return GroupTickets(
            group_id=group['_id'],
            group_name=group['name'],
            tickets=group.get('tickets', [])
        )

    def determine_category(self, group_name: str) -> TicketCategory:
        """Determine the ticket category based on group name"""
        name_lower = group_name.lower()
        if "double" in name_lower:
            return TicketCategory.DOUBLES
        elif "relay" in name_lower:
            return TicketCategory.RELAY
        elif "spectator" in name_lower:
            return TicketCategory.SPECTATOR
        return TicketCategory.SINGLE

class VivenuAPI:
    def __init__(self, token: str):
        self.token = token
        self.base_url = "https://vivenu.com/api"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        }

    def get_events(self):
        response = requests.get(f"{self.base_url}/events", headers=self.headers)
        response.raise_for_status()
        return response.json()

    def get_tickets(self, event_id: str, skip: int = 0, limit: int = 1000):
        params = {
            "status": "VALID,DETAILSREQUIRED",
            "skip": skip,
            "top": limit
        }
        response = requests.get(f"{self.base_url}/tickets", headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

def get_db_session(schema: str, skip_fetch: bool = False):
    """Create database session with specific schema"""
    engine = create_engine("postgresql://postgres:postgres@postgres:5432/vivenu_db")
    
    if not skip_fetch:
        try:
            # Create schema if it doesn't exist and set it as default
            with engine.connect() as conn:
                # Create schema
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            
                # Drop existing tables in the correct order
                conn.execute(text(f"""
                    DROP TABLE IF EXISTS {schema}.summary_report CASCADE;
                    DROP TABLE IF EXISTS {schema}.ticket_type_summary CASCADE;
                    DROP TABLE IF EXISTS {schema}.tickets CASCADE;
                    DROP TABLE IF EXISTS {schema}.events CASCADE;
                """))
                logger.info(f"Dropped existing tables in schema {schema}")

                # Set search path
                conn.execute(text(f"SET search_path TO {schema}"))
                conn.commit()
                
                logger.info(f"Successfully reset schema {schema}")
        except Exception as e:
            logger.error(f"Error setting up schema {schema}: {e}")
            raise

        try:
            # Create all tables in the correct schema
            Base.metadata.schema = schema
            Base.metadata.create_all(engine, checkfirst=False)
            logger.info(f"Successfully created tables in schema {schema}")
        except Exception as e:
            logger.error(f"Error creating tables in schema {schema}: {e}")
            raise

    # Create session with correct schema
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Set search path for this session
    session.execute(text(f"SET search_path TO {schema}"))
    
    return session

def verify_tables(session, schema: str):
    """Verify that tables exist with correct columns"""
    try:
        # Check if ticket_category column exists
        result = session.execute(text(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = '{schema}' 
            AND table_name = 'tickets' 
            AND column_name = 'ticket_category'
        """))
        
        if not result.fetchone():
            logger.error(f"ticket_category column not found in {schema}.tickets")
            raise Exception("Required columns not found")
            
        logger.info(f"Table verification successful for schema {schema}")
    except Exception as e:
        logger.error(f"Table verification failed for schema {schema}: {e}")
        raise

def fetch_ticket_batch(api, event_id, skip, batch_size):
    """Helper function for parallel ticket fetching with rate limiting"""
    max_retries = 3
    retry_delay = 2  # seconds
    
    for attempt in range(max_retries):
        try:
            tickets = api.get_tickets(event_id, skip=skip, limit=batch_size)
            logger.info(f"Successfully fetched batch at offset {skip}")
            return tickets
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Retry {attempt + 1}/{max_retries} for offset {skip}: {e}")
                time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
            else:
                logger.error(f"Failed to fetch tickets batch at offset {skip}: {e}")
                return {"rows": []}

def determine_ticket_group(ticket_name: str) -> str:
    """Determine ticket group based on ticket name"""
    ticket_name_lower = ticket_name.lower()
    if 'double' in ticket_name_lower:
        return 'double'
    elif 'relay' in ticket_name_lower:
        return 'relay'
    return 'single'

def process_ticket_data(session, ticket_data, event_data, schema):
    """Process ticket data and determine ticket category"""
    # Find the matching ticket type in event data to get barcode prefix
    ticket_type = next(
        (ticket for ticket in event_data.get('tickets', [])
         if ticket.get('_id') == ticket_data.get('ticketTypeId')),
        None
    )
    
    if not ticket_type:
        logger.warning(f"Ticket type not found for ticket {ticket_data.get('_id')}")
        return None
        
    barcode_prefix = ticket_type.get('barcodePrefix', '')
    
    # Skip tickets with RF or SP prefix
    if barcode_prefix in ["RF", "SP"]:
        return None
        
    # Determine ticket category based on barcode prefix
    ticket_category = "spectator" if barcode_prefix and barcode_prefix.startswith("S") else "regular"
    
    # Create ticket object
    ticket = Ticket(
        id=ticket_data["_id"],
        region_schema=schema,
        name=ticket_data.get("name"),
        transaction_id=ticket_data.get("transactionId"),
        ticket_type_id=ticket_data.get("ticketTypeId"),
        currency=ticket_data.get("currency"),
        status=ticket_data.get("status"),
        personalized=ticket_data.get("personalized", False),
        expired=ticket_data.get("expired", False),
        event_id=ticket_data.get("eventId"),
        seller_id=ticket_data.get("sellerId"),
        ticket_name=ticket_data.get("ticketName"),
        category_name=ticket_data.get("categoryName"),
        real_price=ticket_data.get("realPrice"),
        regular_price=ticket_data.get("regularPrice"),
        barcode=ticket_data.get("barcode"),
        created_at=parse_datetime(ticket_data.get("createdAt")),
        updated_at=parse_datetime(ticket_data.get("updatedAt")),
        city=ticket_data.get("city"),
        country=ticket_data.get("country"),
        customer_id=ticket_data.get("customerId"),
        email=ticket_data.get("email"),
        firstname=ticket_data.get("firstname"),
        lastname=ticket_data.get("lastname"),
        postal=ticket_data.get("postal"),
        ticket_category=ticket_category
    )
    
    session.merge(ticket)
    return ticket

def parse_datetime(dt_str):
    """Parse datetime string to datetime object"""
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        return None

def update_ticket_summary(session, event_id: str, schema: str):
    """Update ticket type summary for an event"""
    try:
        # Get event details first
        event = session.query(Event).filter(Event.id == event_id).first()
        if not event:
            logger.warning(f"Event {event_id} not found for summary update")
            return

        # Get ticket counts grouped by type and category
        ticket_counts = (
            session.query(
                Ticket.event_id,
                Ticket.ticket_type_id,
                Ticket.ticket_name,
                Ticket.ticket_category,
                func.count().label('total_count')
            )
            .filter(Ticket.event_id == event_id)
            .group_by(
                Ticket.event_id,
                Ticket.ticket_type_id,
                Ticket.ticket_name,
                Ticket.ticket_category
            )
            .all()
        )

        # Update summary records
        for count in ticket_counts:
            summary_id = f"{count.event_id}_{count.ticket_type_id}"
            
            summary = session.get(TicketTypeSummary, summary_id)  # Use session.get() instead of query.get()
            if summary:
                summary.total_count = count.total_count
                summary.event_name = event.name
            else:
                summary = TicketTypeSummary(
                    id=summary_id,
                    event_id=count.event_id,
                    event_name=event.name,
                    ticket_type_id=count.ticket_type_id,
                    ticket_name=count.ticket_name,
                    ticket_category=count.ticket_category,
                    total_count=count.total_count
                )
                session.add(summary)

        session.commit()
        
        # Update summary report after ticket summary is updated
        update_summary_report(session, event_id, schema)
        
        logger.info(f"Updated ticket summary for event: {event_id} in schema: {schema}")

    except Exception as e:
        session.rollback()
        logger.error(f"Error updating ticket summary in schema {schema}: {e}")
        raise

def update_summary_report(session, event_id: str, schema: str):
    """Update summary report with enhanced categorization"""
    try:
        event = session.query(Event).filter(Event.id == event_id).first()
        if not event:
            logger.warning(f"Event {event_id} not found for summary update")
            return

        # Get all existing ticket type summaries for this event
        ticket_summaries = session.query(TicketTypeSummary).filter(
            TicketTypeSummary.event_id == event_id
        ).all()
        
        summary_lookup = {
            summary.ticket_type_id: summary 
            for summary in ticket_summaries
        }

        # Initialize collections for different categories
        category_tickets = defaultdict(set)  # Original categories (Single, Doubles, Relay)
        detailed_category_tickets = defaultdict(set)  # New detailed categories

        # First pass: Process singles and create initial groupings
        for summary in ticket_summaries:
            if "HYROX" in summary.ticket_name:
                name_lower = summary.ticket_name.lower()
                
                # Handle PRO categories separately
                if "pro" in name_lower:
                    if "men" in name_lower:
                        detailed_category_tickets["singles_pro_men"].add(summary.ticket_type_id)
                    elif "women" in name_lower:
                        detailed_category_tickets["singles_pro_women"].add(summary.ticket_type_id)
                    continue

                # Handle regular and adaptive categories
                if "adaptive" in name_lower:
                    if "men" in name_lower:
                        detailed_category_tickets["singles_men_with_adaptive"].add(summary.ticket_type_id)
                    elif "women" in name_lower:
                        detailed_category_tickets["singles_women_with_adaptive"].add(summary.ticket_type_id)
                elif "men" in name_lower:
                    detailed_category_tickets["singles_men_with_adaptive"].add(summary.ticket_type_id)
                elif "women" in name_lower:
                    detailed_category_tickets["singles_women_with_adaptive"].add(summary.ticket_type_id)

        # Process cart automation rules
        for rule in event.cartAutomationRules:
            trigger_group = next(
                (g for g in event.groups if g['_id'] == rule.get('triggerTargetGroup')),
                None
            )
            
            if not trigger_group:
                continue

            trigger_tickets = trigger_group.get('tickets', [])
            group_name = trigger_group.get('name', '').lower()
            
            # Determine basic category
            if 'spectator' in group_name:
                category = 'spectator'
            elif 'double' in group_name:
                category = 'all doubles'
            elif 'relay' in group_name:
                category = 'all relay'
            else:
                category = 'all single'

            # Add to original categories
            if trigger_tickets and all(tid in summary_lookup for tid in trigger_tickets):
                category_tickets[category].update(trigger_tickets)

            # Determine detailed category for doubles and relays
            detailed_category = None
            if 'double' in group_name:
                if 'mixed' in group_name:
                    detailed_category = 'doubles_mixed'
                elif 'women' in group_name:
                    detailed_category = 'doubles_women'
                    if 'pro' in group_name:
                        detailed_category = 'doubles_women_pro'
                elif 'men' in group_name:
                    detailed_category = 'doubles_men'
                    if 'pro' in group_name:
                        detailed_category = 'doubles_men_pro'
            elif 'relay' in group_name:
                if 'mixed' in group_name:
                    detailed_category = 'relay_mixed'
                elif 'women' in group_name:
                    detailed_category = 'relay_women'
                elif 'men' in group_name:
                    detailed_category = 'relay_men'

            # Add to detailed categories
            if detailed_category and trigger_tickets:
                detailed_category_tickets[detailed_category].update(trigger_tickets)

            # Process target groups (similar to original logic)
            for target in rule.get('thenTargets', []):
                then_target_group = next(
                    (g for g in event.groups if g['_id'] == target.get('thenTargetGroup')),
                    None
                )
                
                if then_target_group:
                    target_tickets = then_target_group.get('tickets', [])
                    if target_tickets and all(tid in summary_lookup for tid in target_tickets):
                        category_tickets[category].update(target_tickets)
                        if detailed_category:
                            detailed_category_tickets[detailed_category].update(target_tickets)

        # Create summary reports for original categories
        for category, tickets in category_tickets.items():
            create_summary_report(session, event_id, category, tickets, summary_lookup)

        # Create summary reports for detailed categories
        for category, tickets in detailed_category_tickets.items():
            create_summary_report(session, event_id, category, tickets, summary_lookup)

        # Create "all" category summary
        all_tickets = set()
        for tickets in category_tickets.values():
            all_tickets.update(tickets)
        create_summary_report(session, event_id, "all", all_tickets, summary_lookup)

        session.commit()
        logger.info(f"Updated summary report for event: {event_id}")

    except Exception as e:
        session.rollback()
        logger.error(f"Error updating summary report: {e}")
        raise

def create_summary_report(session, event_id: str, category: str, tickets: Set[str], summary_lookup: Dict[str, TicketTypeSummary]):
    """Helper function to create or update a summary report"""
    if not tickets:
        return

    total_count = sum(
        summary_lookup[tid].total_count
        for tid in tickets
        if tid in summary_lookup
    )
    
    summary_id = f"{event_id}_{category.lower()}"
    summary = session.query(SummaryReport).filter(
        SummaryReport.id == summary_id
    ).first()
    
    if not summary:
        summary = SummaryReport(
            id=summary_id,
            event_id=event_id,
            ticket_type_ids=list(tickets),
            ticket_names=[
                summary_lookup[tid].ticket_name
                for tid in tickets
                if tid in summary_lookup
            ],
            ticket_group=category,
            total_count=total_count
        )
        session.add(summary)
    else:
        summary.total_count = total_count
        summary.ticket_type_ids = list(tickets)
        summary.ticket_names = [
            summary_lookup[tid].ticket_name
            for tid in tickets
            if tid in summary_lookup
        ]
        summary.ticket_group = category
    
    logger.info(f"Updated summary for {category} - Total count: {total_count}")

def get_ticket_summaries(session, event_id: str) -> Dict[str, TicketTypeSummary]:
    """Get ticket type summaries for an event"""
    summaries = session.query(TicketTypeSummary).filter(
        TicketTypeSummary.event_id == event_id
    ).all()
    return {summary.ticket_type_id: summary for summary in summaries}

def process_automation_rules(
    event: Event, 
    grouper: TicketGrouper
) -> Dict[TicketCategory, Set[str]]:
    """Process automation rules and collect tickets by category"""
    category_tickets: Dict[TicketCategory, Set[str]] = defaultdict(set)
    
    for rule in event.cartAutomationRules:
        trigger_group = grouper.create_group_tickets(rule.get('triggerTargetGroup'))
        if not trigger_group:
            continue

        category = grouper.determine_category(trigger_group.group_name)
        
        # Add trigger group tickets
        category_tickets[category].update(trigger_group.tickets)
        
        # Process target groups
        for target in rule.get('thenTargets', []):
            target_group = grouper.create_group_tickets(target.get('thenTargetGroup'))
            if target_group:
                category_tickets[category].update(target_group.tickets)
    
    return category_tickets

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
        groups=event_data.get('groups', [])
    )
    
    try:
        session.merge(event)
        session.commit()
        return event
    except Exception as e:
        session.rollback()
        logger.error(f"Error creating event: {e}")
        raise

def ingest_data(token: str, event_id: str, schema: str, skip_fetch: bool = False):
    """Main ingestion function"""
    api = VivenuAPI(token)
    session = get_db_session(schema, skip_fetch=skip_fetch)
    
    if skip_fetch:
        logger.info("Skipping API fetch. Proceeding with update_ticket_summary and update_summary_report.")
        # Directly call the update functions
        update_ticket_summary(session, event_id, schema)
        update_summary_report(session, event_id, schema)
        session.close()
        return
    
    try:
        # Verify tables are correctly set up
        verify_tables(session, schema)
        
        # Fetch and store events
        events = api.get_events()
        for event_data in events["rows"]:
            event = create_event(session, event_data, schema)

        # Fetch tickets with controlled parallelization
        batch_size = 1000
        max_concurrent_requests = 3  # Limit concurrent requests
        
        # Get first batch to determine total
        first_batch = api.get_tickets(event_id, skip=0, limit=1)
        if not first_batch or "total" not in first_batch:
            logger.warning(f"No tickets found for event {event_id}")
            return

        total_tickets = first_batch["total"]
        logger.info(f"Total tickets to be processed for {schema} schema: {total_tickets}")
        total_batches = ceil(total_tickets / batch_size)
        logger.info(f"Total tickets: {total_tickets}, Total batches: {total_batches}")

        # Process batches in chunks to control concurrency
        processed_tickets = 0
        with ThreadPoolExecutor(max_workers=max_concurrent_requests) as executor:
            for batch_start in range(0, total_batches, max_concurrent_requests):
                # Calculate batch offsets for this chunk
                batch_offsets = [
                    i * batch_size 
                    for i in range(batch_start, min(batch_start + max_concurrent_requests, total_batches))
                ]
                
                logger.info(f"Processing batch offsets: {batch_offsets}")
                
                # Submit batch requests for this chunk
                future_to_offset = {
                    executor.submit(fetch_ticket_batch, api, event_id, offset, batch_size): offset
                    for offset in batch_offsets
                }

                # Process results as they complete
                for future in future_to_offset:
                    try:
                        tickets_response = future.result()
                        tickets = tickets_response.get("rows", [])
                        offset = future_to_offset[future]
                        
                        for ticket_data in tickets:
                            if ticket_data["eventId"] != event_id:
                                continue
                            ticket_type = process_ticket_data(session, ticket_data, events["rows"][0], schema)
                            if ticket_type:
                                processed_tickets += 1

                        # Commit after each batch
                        session.commit()
                        logger.info(f"Processed batch at offset {offset}. "
                                    f"Total progress: {processed_tickets}/{total_tickets} tickets")

                    except Exception as e:
                        logger.error(f"Error processing batch: {e}")
                        session.rollback()

                # Add a small delay between chunks to avoid overwhelming the API
                if batch_start + max_concurrent_requests < total_batches:
                    time.sleep(1)

        # Final commit and summary update
        session.commit()
        update_ticket_summary(session, event_id, schema)
        
        logger.info(f"Data ingestion completed successfully for schema: {schema}. "
                    f"Processed {processed_tickets}/{total_tickets} tickets")

    except Exception as e:
        session.rollback()
        logger.error(f"Error during ingestion for schema {schema}: {e}", exc_info=True)
        raise
    finally:
        session.close()

def get_event_configs():
    """Get all event configurations from environment"""
    import os
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
    from dotenv import load_dotenv
    load_dotenv()
    
    configs = get_event_configs()
    if not configs:
        raise ValueError("No valid event configurations found in environment")
    
    # Process each config (can be run in parallel if needed)
    for config in configs:
        try:
            logger.info(f"Processing schema: {config['schema']}")
            # Set skip_fetch to True if you want to skip fetching from the API
            ingest_data(config["token"], config["event_id"], config["schema"], skip_fetch=False)
        except Exception as e:
            logger.error(f"Failed to process schema {config['schema']}: {e}")
            continue 