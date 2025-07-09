import logging
import re
from typing import Dict, List, Optional, Set

from models.database import TicketUnderShop, TicketVolumes, TicketUnderShopSummary
from datetime import datetime

logger = logging.getLogger(__name__)

class UnderShopProcessor:
    """Processes underShops data from event JSON"""
    
    def __init__(self, session, schema: str):
        self.session = session
        self.schema = schema
        
    def extract_shop_category(self, customer_tags: List[str]) -> Optional[str]:
        if not customer_tags:
            return None
            
        # Look for tags with HTCACCESS or PARTNERACCESS prefix
        for tag in customer_tags:
            # Skip if tag is not a string or is empty
            if not isinstance(tag, str) or not tag:
                continue
                
            # Check if the tag itself is exactly HTCACCESS or PARTNERACCESS
            if tag.upper() == "PARTNERACCESS":
                return "partneraccess"
            
            # Check for HTCACCESS or PARTNERACCESS with suffix (various delimiter styles)
            htc_match = re.match(r'HTCACCESS[-_]([a-zA-Z0-9]+)', tag, re.IGNORECASE)
            partner_match = re.match(r'PARTNERACCESS[-_]([a-zA-Z0-9]+)', tag, re.IGNORECASE)
            
            if htc_match:
                suffix = htc_match.group(1)
                return f"htcaccess-{suffix.lower()}" if suffix else "htcaccess"
            elif partner_match:
                suffix = partner_match.group(1)
                return f"partneraccess-{suffix.lower()}" if suffix else "partneraccess"
                
        return None

    def normalize_shop_name(self, name: str) -> str:
        if not name:
            return ""
        
        # Remove tabs, multiple spaces, and trim
        return re.sub(r'\s+', ' ', name).strip()
        
    def process_under_shops(self, event_data: Dict, event_id: str) -> None:
        try:
            if 'underShops' not in event_data:
                logger.info(f"No underShops found for event {event_id}")
                return
                
            under_shops = event_data.get('underShops', [])
            logger.info(f"Processing {len(under_shops)} underShops for event {event_id}")
            
            shops_processed = 0
            tickets_processed = 0
            
            # Process each underShop
            for shop in under_shops:
                # Skip inactive shops
                if not shop.get('active', False):
                    logger.debug(f"Skipping inactive underShop for event {event_id}")
                    continue
                    
                shop_id = shop.get('_id')
                if not shop_id:
                    logger.warning(f"Skipping underShop without ID for event {event_id}")
                    continue
                
                # Extract shop name and category
                shop_name = self.normalize_shop_name(shop.get('name', ''))
                customer_tags = shop.get('customerTags', [])
                shop_category = self.extract_shop_category(customer_tags)
                
                # Handle shops with required tags
                if shop_category:
                    logger.debug(f"Processing underShop {shop_id} with category {shop_category}")
                    
                    # Create or update shop record
                    self.create_or_update_shop(event_id, shop_id, shop_name, shop_category)
                    shops_processed += 1
                    
                    # Process tickets
                    active_tickets = 0
                    tickets = shop.get('tickets', [])
                    for ticket in tickets:
                        # Skip inactive tickets
                        if not ticket.get('active', False):
                            continue
                            
                        base_ticket = ticket.get('baseTicket')
                        if not base_ticket:
                            logger.warning(f"Skipping ticket without baseTicket in shop {shop_id}")
                            continue
                            
                        # Create ticket volume record
                        volume = ticket.get('amount', 0)
                        ticket_id = ticket.get('_id')
                        
                        self.create_or_update_ticket_volume(
                            event_id=event_id,
                            shop_id=shop_id,
                            ticket_type_id=base_ticket,
                            volume=volume,
                            ticket_id=ticket_id
                        )
                        active_tickets += 1
                        tickets_processed += 1
                    
                    logger.debug(f"Processed {active_tickets} active tickets for shop {shop_id}")
                else:
                    logger.debug(f"Skipping underShop {shop_id} - '{shop_name}' without required HTCACCESS/PARTNERACCESS tag. Tags: {customer_tags}")
                    
            # Commit changes
            self.session.commit()
            logger.info(f"Successfully processed {shops_processed} underShops with {tickets_processed} tickets for event {event_id}")
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error processing underShops for event {event_id}: {str(e)}")
            raise
                    
    def create_or_update_shop(self, event_id: str, shop_id: str, shop_name: str, shop_category: str) -> TicketUnderShop:
        """
        Create or update underShop record
        
        Args:
            event_id: Event ID
            shop_id: Shop ID
            shop_name: Shop name
            shop_category: Shop category
            
        Returns:
            TicketUnderShop instance
        """
        # Check if shop exists
        shop = self.session.query(TicketUnderShop).filter(
            TicketUnderShop.event_id == event_id,
            TicketUnderShop.shop_id == shop_id
        ).first()
        
        if shop:
            # Update existing shop
            shop.shop_name = shop_name
            shop.shop_category = shop_category
            shop.updated_at = datetime.utcnow()
        else:
            # Create new shop
            shop = TicketUnderShop(
                event_id=event_id,
                shop_id=shop_id,
                shop_name=shop_name,
                shop_category=shop_category,
                active=True
            )
            self.session.add(shop)
            
        return shop
        
    def create_or_update_ticket_volume(self, event_id: str, shop_id: str, ticket_type_id: str, 
                                       volume: int, ticket_id: str = None) -> TicketVolumes:
        # Get shop category
        shop = self.session.query(TicketUnderShop).filter(
            TicketUnderShop.event_id == event_id,
            TicketUnderShop.shop_id == shop_id
        ).first()
        
        shop_category = shop.shop_category if shop else 'all'
        
        # Check if volume record exists
        ticket_volume = self.session.query(TicketVolumes).filter(
            TicketVolumes.event_id == event_id,
            TicketVolumes.shop_id == shop_id,
            TicketVolumes.ticket_type_id == ticket_type_id
        ).first()
        
        if ticket_volume:
            # Update existing record
            ticket_volume.volume = volume
            ticket_volume.ticket_shop_category = shop_category
            ticket_volume.updated_at = datetime.utcnow()
        else:
            # Create new record
            ticket_volume = TicketVolumes(
                event_id=event_id,
                shop_id=shop_id,
                ticket_type_id=ticket_type_id,
                volume=volume,
                ticket_shop_category=shop_category,
                active=True
            )
            self.session.add(ticket_volume)
            
        return ticket_volume
            
def update_under_shop_summary(session, schema: str, event_id: str) -> None:
    try:
        # Get event details
        from models.database import Event, Ticket
        from sqlalchemy import func
        
        event = session.query(Event).filter(Event.id == event_id).first()
        if not event:
            logger.warning(f"Event {event_id} not found for under shop summary update")
            return
            
        # Create a lookup dictionary for ticket names using ticket_type_id
        ticket_name_map = {ticket.get('id'): ticket.get('name') for ticket in event.tickets}
        
        # Get ticket counts for under shop tickets grouped by type and shop
        ticket_counts = (
            session.query(
                Ticket.event_id,
                Ticket.ticket_type_id,
                Ticket.under_shop_id,
                Ticket.ticket_name,
                TicketUnderShop.shop_category,
                func.count().label('ticket_count')
            )
            .join(
                TicketUnderShop, 
                Ticket.under_shop_id == TicketUnderShop.shop_id
            )
            .filter(
                Ticket.event_id == event_id,
                Ticket.is_under_shop == True,
                Ticket.under_shop_id != None
            )
            .group_by(
                Ticket.event_id,
                Ticket.ticket_type_id,
                Ticket.under_shop_id,
                Ticket.ticket_name,
                TicketUnderShop.shop_category
            )
            .all()
        )
        
        logger.info(f"Found {len(ticket_counts)} under shop ticket groups for event {event_id}")
        
        from utils.event_processor import determine_ticket_group, determine_ticket_event_day
        
        # Update summary records
        for count in ticket_counts:
            ticket_name = ticket_name_map.get(count.ticket_type_id, count.ticket_name)
            if not ticket_name:
                logger.warning(f"Ticket name not found for {count.ticket_type_id}, using database value: {count.ticket_name}")
                ticket_name = count.ticket_name
                
            summary_id = f"{count.event_id}_{count.ticket_type_id}_{count.under_shop_id}"
            
            # Determine ticket category and event day
            ticket_category = determine_ticket_group(ticket_name).value if ticket_name else None
            ticket_event_day = determine_ticket_event_day(ticket_name).value if ticket_name else None
            
            # Get ticket volume information
            volume_info = session.query(TicketVolumes).filter(
                TicketVolumes.event_id == count.event_id,
                TicketVolumes.shop_id == count.under_shop_id,
                TicketVolumes.ticket_type_id == count.ticket_type_id
            ).first()
            
            ticket_volume = volume_info.volume if volume_info else 0
            
            summary = session.query(TicketUnderShopSummary).filter(
                TicketUnderShopSummary.id == summary_id
            ).first()
            
            if summary:
                summary.ticket_count = count.ticket_count
                summary.ticket_volume = ticket_volume
                summary.updated_at = datetime.utcnow()
                logger.debug(f"Updated summary for {summary_id}: {count.ticket_count} tickets, volume: {ticket_volume}")
            else:
                summary = TicketUnderShopSummary(
                    id=summary_id,
                    event_id=count.event_id,
                    event_name=event.name,
                    ticket_type_id=count.ticket_type_id,
                    ticket_name=ticket_name,
                    ticket_category=ticket_category,
                    ticket_event_day=ticket_event_day,
                    under_shop_id=count.under_shop_id,
                    shop_category=count.shop_category,
                    ticket_count=count.ticket_count,
                    ticket_volume=ticket_volume
                )
                session.add(summary)
                logger.debug(f"Created new summary for {summary_id}: {count.ticket_count} tickets, volume: {ticket_volume}")
                
        session.commit()
        logger.info(f"Updated under shop summary for event: {event_id} in schema: {schema}")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error updating under shop summary in schema {schema}: {e}")
        raise 