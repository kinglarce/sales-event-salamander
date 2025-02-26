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
import os
from dotenv import load_dotenv

# Create logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Set up logging to both file and console
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create file handler with timestamp in filename
current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
file_handler = logging.FileHandler(f'logs/ingest_{current_time}.log')
file_handler.setLevel(logging.DEBUG)

# Create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

class TicketCategory(Enum):
    SINGLE = "single"
    DOUBLES = "double"
    RELAY = "relay"
    SPECTATOR = "spectator"

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

    def get_tickets(self, skip: int = 0, limit: int = 1000):
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

def determine_ticket_group(ticket_name: str) -> TicketCategory:
    """Determine basic ticket group (single, double, relay, spectator)"""
    name_lower = ticket_name.lower()
    if 'double' in name_lower:
        return TicketCategory.DOUBLES
    elif 'relay' in name_lower:
        return TicketCategory.RELAY
    elif 'spectator' in name_lower:
        return TicketCategory.SPECTATOR
    return TicketCategory.SINGLE

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

def process_ticket_data(session, ticket_data, event_data, schema, target_event_id):
    """Process ticket data and store in database"""
    try:
        ticket_id = ticket_data.get("_id")
        event_id = event_data.get("_id")

        if LogConfig.DEBUG_ENABLED:
            logger.debug(f"Schema: {schema}, Event ID: {event_id}, Ticket ID: {ticket_id}")

        # Skip if this ticket is not for our target event
        if event_id != ticket_data.get("eventId"):
            logger.debug(f"Skipping ticket {ticket_id} - not for target event {target_event_id}")
            return None

        # Check if event exists in database
        event = session.query(Event).filter(Event.id == event_id).first()
        if not event:
            logger.debug(f"Skipping ticket {ticket_id} - event {event_id} not found in database")
            return None
        

        try:
            ticket = Ticket(
                id=ticket_data.get("_id"),
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
                barcode=ticket_data.get("barcode"),
                created_at=ticket_data.get("createdAt"),
                updated_at=ticket_data.get("updatedAt"),
                city=ticket_data.get("city"),
                country=ticket_data.get("country"),
                customer_id=ticket_data.get("customerId"),
                email=ticket_data.get("email"),
                firstname=ticket_data.get("firstname"),
                lastname=ticket_data.get("lastname"),
                postal=ticket_data.get("postal"),
                ticket_category="spectator" if "SPECTATOR" in ticket_data.get("ticketName", "").upper() else "regular"
            )
            
            logger.debug(f"Created ticket object for {ticket_id}")
            
            session.merge(ticket)
            session.flush()
            logger.debug(f"Successfully merged and flushed ticket {ticket_id}")
            return ticket

        except Exception as e:
            logger.error(f"""
            =======
            Failed to process ticket:
            Schema: {schema}
            Event ID: {event_id}
            Ticket ID: {ticket_id}
            Full ticket data: {ticket_data}
            Error: {str(e)}
            =======
            """)
            session.rollback()
            return None

    except Exception as e:
        logger.error(f"Error in process_ticket_data: {str(e)}", exc_info=True)
        return None

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
            
            summary = session.get(TicketTypeSummary, summary_id)
            if summary:
                summary.total_count = count.total_count
                summary.event_name = event.name
                # Update group_name if it's null
                if not summary.group_name:
                    summary.group_name = determine_ticket_group(count.ticket_name).value
            else:
                summary = TicketTypeSummary(
                    id=summary_id,
                    event_id=count.event_id,
                    event_name=event.name,
                    ticket_type_id=count.ticket_type_id,
                    ticket_name=count.ticket_name,
                    ticket_category=count.ticket_category,
                    total_count=count.total_count,
                    group_name=determine_ticket_group(count.ticket_name).value
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
    """Update summary report with only basic categories"""
    try:
        # Get all ticket type summaries for this event
        summaries = session.query(TicketTypeSummary).filter(
            TicketTypeSummary.event_id == event_id,
            TicketTypeSummary.ticket_name.ilike('HYROX%') | 
            TicketTypeSummary.ticket_name.ilike('%Spectator%')
        ).all()

        # Initialize collections for different categories
        basic_groups = defaultdict(set)  # For single, double, relay, spectator
        
        # First clear existing summary reports for this event
        session.query(SummaryReport).filter(
            SummaryReport.event_id == event_id
        ).delete()
        
        # Process each summary and add to basic groups
        for summary in summaries:
            basic_group = determine_ticket_group(summary.ticket_name).value
            basic_groups[basic_group].add(summary.ticket_type_id)
            
            # Also add spectator tickets to their own group
            if basic_group == 'spectator':
                basic_groups['Spectators'].add(summary.ticket_type_id)

        # Create summary reports for basic groups
        for group, tickets in basic_groups.items():
            if group == 'spectator':
                continue  # Skip the lowercase 'spectator' as we use 'Spectator' instead
                
            group_name = group
            if group != 'Spectators':
                group_name = f"All_{group}s"
                
            total = sum(
                summary.total_count 
                for summary in summaries 
                if summary.ticket_type_id in tickets
            )
            
            # Gather ticket names for the current group
            ticket_names = [
                summary.ticket_name 
                for summary in summaries 
                if summary.ticket_type_id in tickets
            ]

            create_summary_report(
                session,
                event_id,
                group_name,
                tickets,
                total,
                ticket_names
            )

        # Create total summary (excluding spectators)
        non_spectator_tickets = set()
        non_spectator_total = 0
        for summary in summaries:
            if not summary.ticket_name.upper().__contains__('SPECTATOR'):
                non_spectator_tickets.add(summary.ticket_type_id)
                non_spectator_total += summary.total_count

        # Gather ticket names for the total excluding spectators
        total_ticket_names = [
            summary.ticket_name 
            for summary in summaries 
            if summary.ticket_type_id in non_spectator_tickets
        ]

        create_summary_report(
            session,
            event_id,
            "Total_excluding_spectators",
            non_spectator_tickets,
            non_spectator_total,
            total_ticket_names
        )

        session.commit()
        logger.info(f"Updated summary report for event: {event_id}")

    except Exception as e:
        session.rollback()
        logger.error(f"Error updating summary report: {e}")
        raise

def create_summary_report(session, event_id: str, category: str, tickets: Set[str], total_count: int, ticket_names: List[str]):
    """Helper function to create or update a summary report"""
    if not tickets:
        return

    summary_id = f"{event_id}_{category.lower()}"
    
    summary = SummaryReport(
        id=summary_id,
        event_id=event_id,
        ticket_type_ids=list(tickets),
        ticket_group=category,
        total_count=total_count,
        ticket_names=ticket_names  # Include ticket names in the summary report
    )
    session.add(summary)
    
    logger.info(f"Created summary for {category} - Total count: {total_count}")

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

def process_batch(session, tickets, event_data, schema, target_event_id):
    processed = 0
    for ticket in tickets:
        result = process_ticket_data(session, ticket, event_data, schema, target_event_id)
        if result:
            processed += 1

    return processed

def ingest_data(token: str, event_id: str, schema: str, skip_fetch: bool = False, debug: bool = False):
    """Main ingestion function"""
    # Set debug logging
    LogConfig.set_debug(debug)
    
    api = VivenuAPI(token)
    session = get_db_session(schema, skip_fetch=skip_fetch)
    
    if skip_fetch:
        logger.info("Skipping API fetch. Proceeding with update_ticket_summary and update_summary_report.")
        update_ticket_summary(session, event_id, schema)
        update_summary_report(session, event_id, schema)
        session.close()
        return
    
    try:
        # Verify tables are correctly set up
        verify_tables(session, schema)
        
        # Fetch and store events
        events = api.get_events()
        event = None
        found_event_data = None
        for event_data in events["rows"]:
            if event_data.get("_id") == event_id:
                event = create_event(session, event_data, schema)
                found_event_data = event_data
                logger.info(f"Found matching event: {event_id}")
                break
        
        if not event:
            logger.error(f"Event {event_id} not found in fetched events")
            return

        # Fetch tickets with controlled parallelization
        batch_size = 1000
        max_concurrent_requests = 3
        processed_tickets = 0
        
        # Get first batch to determine total
        first_batch = api.get_tickets(skip=0, limit=1)
        if not first_batch or "total" not in first_batch:
            logger.warning(f"No tickets found for event {event_id}")
            return

        total_tickets = first_batch["total"]
        total_batches = ceil(total_tickets / batch_size)
        logger.info(f"Total tickets to be processed for {schema} schema: {total_tickets}")
        logger.info(f"Total tickets: {total_tickets}, Total batches: {total_batches}")

        # Process batches in chunks to control concurrency
        with ThreadPoolExecutor(max_workers=max_concurrent_requests) as executor:
            for batch_start in range(0, total_batches, max_concurrent_requests):
                batch_offsets = [
                    i * batch_size 
                    for i in range(batch_start, min(batch_start + max_concurrent_requests, total_batches))
                ]
                
                logger.info(f"Processing batch offsets: {batch_offsets}")
                
                future_to_offset = {
                    executor.submit(api.get_tickets, offset, batch_size): offset
                    for offset in batch_offsets
                }

                for future in future_to_offset:
                    try:
                        tickets_response = future.result()
                        tickets = tickets_response.get("rows", [])
                        offset = future_to_offset[future]
                        
                        logger.info(f"Processing {len(tickets)} tickets from offset {offset}")
                        batch_processed = process_batch(session, tickets, found_event_data, schema, event_id)
                        processed_tickets += batch_processed
                        
                        # Commit after each batch
                        try:
                            session.commit()
                            logger.info(f"Processed batch at offset {offset}. "
                                      f"Batch processed: {batch_processed}, "
                                      f"Total progress: {processed_tickets}/{total_tickets} tickets")
                        except Exception as e:
                            logger.error(f"Error committing batch: {e}")
                            session.rollback()

                    except Exception as e:
                        logger.error(f"Error processing batch: {e}", exc_info=True)
                        session.rollback()

        # Final commit and summary update
        session.commit()
        logger.info(f"Successfully processed {processed_tickets} tickets for event {event_id}")
        
        # Update summaries
        update_ticket_summary(session, event_id, schema)
        update_summary_report(session, event_id, schema)
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error during ingestion for schema {schema}: {e}", exc_info=True)
        raise
    finally:
        session.close()

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