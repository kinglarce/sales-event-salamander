"""
Gender Fix Ingestion v2
Refactored with senior software engineering best practices.
"""

import asyncio
import logging
import json
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
    PerformanceLogger,
    HTTPClientFactory,
    AsyncRetryingClient
)

logger = get_logger(__name__)


@dataclass
class GenderFixResult:
    """Result of gender fix processing"""
    schema: str
    region: str
    total_tickets_analyzed: int
    tickets_needing_update: int
    gender_breakdown: Dict[str, int]
    duration: float
    success: bool
    error_message: Optional[str] = None


class GenderDeterminer:
    """Determines gender based on ticket names using the same logic as the SQL reports"""
    
    @staticmethod
    def determine_gender_from_ticket_name(ticket_name: str) -> Optional[str]:
        """
        Determine gender from ticket name using the same logic as get_detailed_summary_with_day_report.sql
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


class GenderFixProcessor:
    """Processes tickets to identify and fix missing gender fields"""
    
    def __init__(self, session, schema: str, event_id: str, event_day: str):
        self.session = session
        self.schema = schema
        self.event_id = event_id
        self.event_day = event_day
        self.processed = 0
        self.failed = 0
        self.gender_determined = 0
        self.needs_update = 0
        self.gender_mapper = GenderDeterminer()
        self.logger = get_logger(__name__)

    def is_valid_athlete_ticket(self, ticket: Dict) -> bool:
        """
        Check if ticket is a valid HYROX athlete ticket that should be processed.
        """
        try:
            ticket_name = ticket.get('ticketName', '')
            event_id = ticket.get('eventId')

            if not ticket_name:
                return False

            # First filter: Check if ticket is from the specified event
            if event_id != self.event_id:
                return False
            
            # Second filter: Check if ticket is for the specified event day
            # This would need to be implemented based on your event day logic
            # For now, we'll assume all tickets are valid for the specified day
            
            # Third filter: Check if it's a HYROX ticket
            if not ticket_name.upper().startswith('HYROX'):
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking ticket validity: {e}")
            return False

    def process_ticket_for_gender_analysis(self, ticket: Dict) -> Optional[Dict]:
        """
        Process a single ticket for gender analysis.
        """
        try:
            # Extract basic ticket info
            ticket_id = ticket.get('_id')
            ticket_name = ticket.get('ticketName', '')
            extra_fields = ticket.get('extraFields', {})
            
            # Skip if no ticket name
            if not ticket_name:
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
                self.logger.debug(f"Ticket {ticket_id} needs gender update: {current_gender} -> {determined_gender}")
            
            return {
                'id': ticket_id,
                'ticket_name': ticket_name,
                'extra_fields': extra_fields,
                'determined_gender': determined_gender,
                'needs_update': needs_update,
                'current_gender': current_gender
            }
            
        except Exception as e:
            self.logger.error(f"Error processing ticket {ticket.get('_id', 'unknown')}: {e}")
            return None

    def store_gender_analysis(self, analysis_data: Dict):
        """Store gender analysis data in the database"""
        try:
            # Insert or update the analysis record
            insert_sql = f"""
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
            """
            
            self.session.execute(insert_sql, {
                'id': analysis_data['id'],
                'ticket_name': analysis_data['ticket_name'],
                'extra_fields': json.dumps(analysis_data['extra_fields']),
                'determined_gender': analysis_data['determined_gender'],
                'needs_update': analysis_data['needs_update'],
                'current_gender': analysis_data['current_gender']
            })
            
        except Exception as e:
            self.logger.error(f"Error storing gender analysis for ticket {analysis_data['id']}: {e}")
            raise

    def get_gender_analysis_summary(self) -> Dict:
        """Get summary of gender analysis results"""
        try:
            # Get total processed tickets
            result = self.session.execute(
                f"SELECT COUNT(*) as total FROM {self.schema}.tickets_gender_analysis"
            )
            total_processed = result.scalar()
            
            # Get tickets needing updates
            result = self.session.execute(
                f"SELECT COUNT(*) as total FROM {self.schema}.tickets_gender_analysis WHERE needs_update = true"
            )
            tickets_needing_update = result.scalar()
            
            # Get gender breakdown
            result = self.session.execute(
                f"""
                    SELECT determined_gender, COUNT(*) as count 
                    FROM {self.schema}.tickets_gender_analysis 
                    GROUP BY determined_gender 
                    ORDER BY count DESC
                """
            )
            gender_breakdown = {row.determined_gender: row.count for row in result}
            
            return {
                'total_tickets_processed': total_processed,
                'tickets_needing_update': tickets_needing_update,
                'gender_breakdown': gender_breakdown
            }
            
        except Exception as e:
            self.logger.error(f"Error getting summary: {e}")
            return {}


class GenderFixIngester:
    """Handles gender fix ingestion"""
    
    def __init__(self, config, db_manager: DatabaseManager):
        self.config = config
        self.db_manager = db_manager
        self.logger = get_logger(__name__)
        self.performance_logger = PerformanceLogger(self.logger)
    
    def setup_gender_analysis_table(self, schema: str):
        """Set up a temporary table for storing tickets with missing gender for analysis"""
        try:
            with TransactionManager(self.db_manager) as session:
                # Drop the existing table if it exists to start fresh
                session.execute(f"DROP TABLE IF EXISTS {schema}.tickets_gender_analysis CASCADE")
                
                # Create a fresh table for gender analysis
                session.execute(f"""
                    CREATE TABLE {schema}.tickets_gender_analysis (
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
                """)
                
            self.logger.info(f"Successfully dropped and recreated gender analysis table for {schema}")
        except Exception as e:
            self.logger.error(f"Error setting up gender analysis table for {schema}: {e}")
            raise
    
    async def process_tickets_for_gender_analysis(self, schema: str, event_id: str, event_day: str) -> GenderFixResult:
        """Process tickets for gender analysis"""
        start_time = time.time()
        
        try:
            self.performance_logger.start_timer(f"gender_fix_{schema}")
            
            # Setup table
            self.setup_gender_analysis_table(schema)
            
            # Create HTTP client
            client = HTTPClientFactory.create_client(
                headers=self.config.get_headers(),
                verify_ssl=self.config.http_client.verify_ssl,
                timeout_connect=self.config.http_client.timeout_connect,
                timeout_read=self.config.http_client.timeout_read,
                timeout_write=self.config.http_client.timeout_write,
                timeout_pool=self.config.http_client.timeout_pool,
                max_keepalive_connections=self.config.http_client.max_keepalive_connections,
                max_connections=self.config.http_client.max_connections,
                keepalive_expiry=self.config.http_client.keepalive_expiry,
                retries=self.config.http_client.retries,
                http2=self.config.http_client.http2
            )
            retrying_client = AsyncRetryingClient(client)
            
            # Get total tickets
            url = f"{self.config.api.base_url}/tickets"
            params = {
                "status": "VALID,DETAILSREQUIRED",
                "skip": 0,
                "top": 1
            }
            
            response = await retrying_client.get(url, params=params)
            ticket_data = response.json()
            total_tickets = ticket_data.get("total", 0)
            
            if not total_tickets:
                self.logger.warning(f"No tickets found for event {event_id}")
                return GenderFixResult(
                    schema=schema,
                    region="unknown",
                    total_tickets_analyzed=0,
                    tickets_needing_update=0,
                    gender_breakdown={},
                    duration=time.time() - start_time,
                    success=True
                )
            
            # Process tickets in batches
            batch_size = 1000
            total_batches = (total_tickets + batch_size - 1) // batch_size
            
            self.logger.info(f"Processing {total_tickets} tickets in {total_batches} batches for gender analysis")
            
            processed_tickets = 0
            tickets_needing_update = 0
            
            for batch_num in range(total_batches):
                skip = batch_num * batch_size
                limit = batch_size
                
                # Fetch batch
                params = {
                    "status": "VALID,DETAILSREQUIRED",
                    "skip": skip,
                    "top": limit
                }
                
                response = await retrying_client.get(url, params=params)
                ticket_data = response.json()
                tickets = ticket_data.get("rows", [])
                
                if not tickets:
                    break
                
                # Process batch
                with TransactionManager(self.db_manager) as session:
                    processor = GenderFixProcessor(session, schema, event_id, event_day)
                    
                    for ticket in tickets:
                        analysis_data = processor.process_ticket_for_gender_analysis(ticket)
                        if analysis_data:
                            processor.store_gender_analysis(analysis_data)
                            processed_tickets += 1
                            
                            if analysis_data['needs_update']:
                                tickets_needing_update += 1
                
                self.logger.info(f"Batch {batch_num + 1}/{total_batches} processed {len(tickets)} tickets")
            
            # Get final summary
            with TransactionManager(self.db_manager) as session:
                processor = GenderFixProcessor(session, schema, event_id, event_day)
                summary = processor.get_gender_analysis_summary()
            
            duration = time.time() - start_time
            self.performance_logger.end_timer(f"gender_fix_{schema}")
            
            return GenderFixResult(
                schema=schema,
                region="unknown",
                total_tickets_analyzed=summary.get('total_tickets_processed', 0),
                tickets_needing_update=summary.get('tickets_needing_update', 0),
                gender_breakdown=summary.get('gender_breakdown', {}),
                duration=duration,
                success=True
            )
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"Error processing gender fix for {schema}: {e}")
            self.performance_logger.end_timer(f"gender_fix_{schema}", success=False)
            
            return GenderFixResult(
                schema=schema,
                region="unknown",
                total_tickets_analyzed=0,
                tickets_needing_update=0,
                gender_breakdown={},
                duration=duration,
                success=False,
                error_message=str(e)
            )
        finally:
            # Clean up HTTP client
            try:
                await HTTPClientFactory.close_client(client)
            except Exception as e:
                self.logger.debug(f"Error closing HTTP client: {e}")


async def main_ingest_gender_fix():
    """Main entry point for gender fix ingestion"""
    config = get_config()
    db_manager = DatabaseManager()
    ingester = GenderFixIngester(config, db_manager)
    
    results = []
    
    # Process each event configuration
    for event_config in config.events:
        try:
            result = await ingester.process_tickets_for_gender_analysis(
                schema=event_config.schema,
                event_id=event_config.event_id,
                event_day="FRIDAY"  # This should be configurable
            )
            results.append(result)
            
            if result.success:
                logger.info(f"Gender fix processed for {event_config.schema}:")
                logger.info(f"  Total tickets analyzed: {result.total_tickets_analyzed}")
                logger.info(f"  Tickets needing update: {result.tickets_needing_update}")
                logger.info(f"  Gender breakdown: {result.gender_breakdown}")
            else:
                logger.error(f"Failed to process gender fix for {event_config.schema}: {result.error_message}")
                
        except Exception as e:
            logger.error(f"Error processing gender fix for {event_config.schema}: {e}")
            continue
    
    return results


if __name__ == "__main__":
    asyncio.run(main_ingest_gender_fix())
