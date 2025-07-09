import logging
import hashlib
from typing import Dict, List, Optional
from pathlib import Path

from sqlalchemy import text
from models.database import TicketAddonSummary

logger = logging.getLogger(__name__)


class AddonProcessor:
    """Processes ticket addOns data for summary aggregation"""
    
    def __init__(self, session, schema: str):
        self.session = session
        self.schema = schema
    
    def process_ticket_addons(self, ticket_data: Dict) -> Optional[str]:
        """Extract addon name from ticket data - simplified to just return the name"""
        addons = ticket_data.get('addOns', [])
        if not addons:
            return None
            
        # Take the first addon name if multiple exist
        # Most tickets seem to have only one addon anyway
        for addon in addons:
            if isinstance(addon, dict) and addon.get('name'):
                addon_name = addon.get('name', '').strip()
                if addon_name:
                    logger.debug(f"Found addon: {addon_name}")
                    return addon_name
        
        return None
    
    @staticmethod
    def generate_summary_id(event_id: str, addon_name: str) -> str:
        """Generate unique ID for addon summary record"""
        return hashlib.md5(f"{event_id}_{addon_name}".encode()).hexdigest()


def update_addon_summary(session, schema: str, event_id: str) -> None:
    """Update addon summary with current ticket addon counts"""
    try:
        from models.database import Event
        
        # Set the search path for this session
        session.execute(text(f"SET search_path TO {schema}"))
        
        event = session.query(Event).filter(Event.id == event_id).first()
        if not event:
            logger.warning(f"Event {event_id} not found for addon summary update")
            return
            
        logger.info(f"Updating addon summary for event: {event_id} in schema: {schema}")
        
        # Clear existing summaries for this event
        session.query(TicketAddonSummary).filter(
            TicketAddonSummary.event_id == event_id
        ).delete()
        
        # Get addon counts using simplified query
        addon_data = _get_addon_counts_from_database(session, schema, event_id)
        
        if not addon_data:
            logger.info(f"No addon data found for event: {event_id}")
            session.commit()
            return
        
        # Create summary records
        for data in addon_data:
            summary_id = AddonProcessor.generate_summary_id(event_id, data['addon_name'])
            
            summary = TicketAddonSummary(
                id=summary_id,
                event_id=event_id,
                event_name=event.name,
                addon_name=data['addon_name'],
                product_id=None,  # Not needed with simplified approach
                total_count=data['total_count']
            )
            session.add(summary)
            
        session.commit()
        logger.info(f"Updated addon summary: {len(addon_data)} addon types processed")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error updating addon summary in schema {schema}: {e}")
        raise


def _get_addon_counts_from_database(session, schema: str, event_id: str) -> List[Dict]:
    """Execute simplified addon aggregation query"""
    try:
        # Simplified query that just counts by addon name
        query_sql = f"""
            SELECT 
                addons as addon_name,
                COUNT(*) as total_count
            FROM {schema}.tickets 
            WHERE event_id = :event_id 
            AND addons IS NOT NULL 
            AND addons != ''
            GROUP BY addons
            ORDER BY COUNT(*) DESC
        """
        
        query = text(query_sql)
        
        logger.debug(f"Executing addon query for schema {schema}: {query_sql}")
        
        result = session.execute(query, {"event_id": event_id}).fetchall()
        
        logger.info(f"Found {len(result)} addon types for event {event_id} in schema {schema}")
        
        return [
            {
                'addon_name': row.addon_name,
                'total_count': row.total_count
            }
            for row in result
        ]
    except Exception as e:
        logger.error(f"Error executing addon summary query for schema {schema}: {e}")
        return []


def debug_addon_storage(session, schema: str, event_id: str) -> None:
    """Debug function to check if addons are being stored"""
    try:
        session.execute(text(f"SET search_path TO {schema}"))
        
        # Check how many tickets have addons
        query = text(f"""
            SELECT 
                COUNT(*) as total_tickets,
                COUNT(CASE WHEN addons IS NOT NULL AND addons != '' THEN 1 END) as tickets_with_addons
            FROM {schema}.tickets 
            WHERE event_id = :event_id
        """)
        
        result = session.execute(query, {"event_id": event_id}).fetchone()
        logger.info(f"Debug addon storage - Total tickets: {result.total_tickets}, "
                   f"With addons: {result.tickets_with_addons}")
        
        # Sample some addon data
        sample_query = text(f"""
            SELECT id, addons
            FROM {schema}.tickets 
            WHERE event_id = :event_id 
            AND addons IS NOT NULL 
            AND addons != ''
            LIMIT 5
        """)
        
        sample_results = session.execute(sample_query, {"event_id": event_id}).fetchall()
        for row in sample_results:
            logger.info(f"Sample ticket {row.id} addons: {row.addons}")
            
    except Exception as e:
        logger.error(f"Error in debug addon storage: {e}") 