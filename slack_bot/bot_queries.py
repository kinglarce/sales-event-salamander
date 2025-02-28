from typing import Dict, List, Any, Optional, Tuple
import logging
from datetime import datetime, timedelta
from sqlalchemy import func
from sqlalchemy.orm import Session
from models.database import Event, Ticket, TicketTypeSummary, SummaryReport
from slack_bot.database import DatabaseManager

logger = logging.getLogger(__name__)

class BotQueries:
    """Handles all ticket-related database queries using SQLAlchemy models"""
    
    def __init__(self, schema: str, event_id: str = None):
        self.db = DatabaseManager(schema)
        self.schema = schema  # Keep this for reference
        self.event_id = event_id

    def get_sales_trend(self, days: int = 7) -> Dict[str, int]:
        """Get sales trend data using the Ticket model"""
        try:
            days_ago = datetime.now() - timedelta(days=days)
            with self.db.get_session() as session:
                sales_by_day = (
                    session.query(
                        func.date(Ticket.created_at).label('sale_date'),
                        func.count().label('daily_sales')
                    )
                    .filter(
                        Ticket.created_at >= days_ago,
                        Ticket.event_id == self.event_id if self.event_id else True
                    )
                    .group_by(func.date(Ticket.created_at))
                    .order_by(func.date(Ticket.created_at))
                    .all()
                )
                return {str(row.sale_date): row.daily_sales for row in sales_by_day}
        except Exception as e:
            logger.error(f"Error getting sales trend: {e}")
            return {}

    def get_ticket_counts(self) -> Dict[str, int]:
        """Get ticket counts using TicketTypeSummary model"""
        try:
            with self.db.get_session() as session:
                results = (
                    session.query(
                        TicketTypeSummary.ticket_category,
                        func.sum(TicketTypeSummary.total_count).label('count')
                    )
                    .filter(TicketTypeSummary.event_id == self.event_id if self.event_id else True)
                    .group_by(TicketTypeSummary.ticket_category)
                    .all()
                )
                return {row.ticket_category: row.count for row in results}
        except Exception as e:
            logger.error(f"Error getting ticket counts: {e}")
            return {}

    def search_registrants(self, search_term: str, limit: int = 5) -> List[Ticket]:
        """Search registrants using Ticket model"""
        try:
            with self.db.get_session() as session:
                return (
                    session.query(Ticket)
                    .filter(
                        (Ticket.email.ilike(f"%{search_term}%")) |
                        (Ticket.transaction_id.ilike(f"%{search_term}%")) |
                        (Ticket.barcode.ilike(f"%{search_term}%")),
                        Ticket.event_id == self.event_id if self.event_id else True
                    )
                    .limit(limit)
                    .all()
                )
        except Exception as e:
            logger.error(f"Error searching registrants: {e}")
            return []

    def get_ticket_categories(self) -> List[str]:
        """Get ticket categories using TicketTypeSummary model"""
        try:
            with self.db.get_session() as session:
                categories = (
                    session.query(TicketTypeSummary.ticket_category)
                    .distinct()
                    .all()
                )
                return [row.ticket_category for row in categories]
        except Exception as e:
            logger.error(f"Error getting ticket categories: {e}")
            return []

    def get_event_info(self) -> Optional[Event]:
        """Get event information using Event model"""
        try:
            with self.db.get_session() as session:
                return (
                    session.query(Event)
                    .filter(Event.event_id == self.event_id if self.event_id else True)
                    .first()
                )
        except Exception as e:
            logger.error(f"Error getting event info: {e}")
            return None

    def get_ticket_details(self, category: str) -> List[Any]:
        """Get ticket details using TicketTypeSummary model"""
        try:
            with self.db.get_session() as session:
                return (
                    session.query(
                        TicketTypeSummary.ticket_name,
                        TicketTypeSummary.ticket_event_day,
                        func.sum(TicketTypeSummary.total_count).label('count')
                    )
                    .filter(TicketTypeSummary.ticket_category == category)
                    .group_by(
                        TicketTypeSummary.ticket_name,
                        TicketTypeSummary.ticket_event_day
                    )
                    .order_by(
                        TicketTypeSummary.ticket_event_day,
                        TicketTypeSummary.ticket_name
                    )
                    .all()
                )
        except Exception as e:
            logger.error(f"Error getting ticket details: {e}")
            return []

    def get_hourly_sales(self, start_date: datetime, end_date: datetime) -> List[Any]:
        """Get hourly sales data using Ticket model"""
        try:
            with self.db.get_session() as session:
                return (
                    session.query(
                        func.extract('hour', Ticket.created_at).label('hour'),
                        func.count().label('count')
                    )
                    .filter(
                        Ticket.created_at.between(start_date, end_date)
                    )
                    .group_by(func.extract('hour', Ticket.created_at))
                    .order_by(func.extract('hour', Ticket.created_at))
                    .all()
                )
        except Exception as e:
            logger.error(f"Error getting hourly sales: {e}")
            return []

    def get_daily_sales(self, start_date: datetime, end_date: datetime) -> List[Any]:
        """Get daily sales data using Ticket model"""
        try:
            with self.db.get_session() as session:
                return (
                    session.query(
                        func.extract('dow', Ticket.created_at).label('day_of_week'),
                        func.count().label('count')
                    )
                    .filter(
                        Ticket.created_at.between(start_date, end_date)
                    )
                    .group_by(func.extract('dow', Ticket.created_at))
                    .order_by(func.extract('dow', Ticket.created_at))
                    .all()
                )
        except Exception as e:
            logger.error(f"Error getting daily sales: {e}")
            return []

    def get_current_summary(self) -> Dict[str, int]:
        """Get current summary report data using SummaryReport model"""
        try:
            with self.db.get_session() as session:
                latest_summary = (
                    session.query(
                        SummaryReport.ticket_group,
                        SummaryReport.total_count
                    )
                    .filter(SummaryReport.event_id == self.event_id)
                    .order_by(
                        SummaryReport.ticket_group,
                        SummaryReport.created_at.desc()
                    )
                    .distinct(SummaryReport.ticket_group)
                    .all()
                )
                return {row.ticket_group: row.total_count for row in latest_summary}
        except Exception as e:
            logger.error(f"Error getting current summary: {e}")
            return {}

    def get_category_distribution(self) -> List[Dict[str, Any]]:
        """Get ticket category distribution using TicketTypeSummary model"""
        try:
            with self.db.get_session() as session:
                results = (
                    session.query(
                        TicketTypeSummary.ticket_category,
                        func.sum(TicketTypeSummary.total_count).label('total'),
                        func.count(TicketTypeSummary.ticket_type_id.distinct()).label('type_count')
                    )
                    .group_by(TicketTypeSummary.ticket_category)
                    .order_by(func.sum(TicketTypeSummary.total_count).desc())
                    .all()
                )
                return [
                    {
                        'category': row.ticket_category,
                        'total': row.total,
                        'type_count': row.type_count
                    } for row in results
                ]
        except Exception as e:
            logger.error(f"Error getting category distribution: {e}")
            return []
