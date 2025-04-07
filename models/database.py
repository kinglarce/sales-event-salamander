from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, JSON, Boolean, UniqueConstraint, MetaData, Float, ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.sql import text

# Create a base class that will use schema-bound metadata
metadata = MetaData()
Base = declarative_base(metadata=metadata)

class Event(Base):
    __tablename__ = "events"
    
    id = Column(String, primary_key=True)
    region_schema = Column(String, nullable=False)
    name = Column(String)
    seller_id = Column(String)
    location_name = Column(String)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    sell_start = Column(DateTime)
    sell_end = Column(DateTime)
    timezone = Column(String)
    cartAutomationRules = Column(JSON, default=[])
    groups = Column(JSON)
    tickets = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)

class Ticket(Base):
    __tablename__ = "tickets"
    __table_args__ = (
        UniqueConstraint('id', name='tickets_pkey'),
        {'schema': None}
    )
    
    id = Column(String, primary_key=True)
    region_schema = Column(String)
    transaction_id = Column(String)
    ticket_type_id = Column(String)
    currency = Column(String)
    status = Column(String)
    personalized = Column(Boolean)
    expired = Column(Boolean)
    event_id = Column(String, ForeignKey("events.id"))
    ticket_name = Column(String)
    category_name = Column(String)
    barcode = Column(String)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    city = Column(String)
    country = Column(String)
    customer_id = Column(String)
    gender = Column(String)
    birthday = Column(String)
    age = Column(Integer)
    nationality = Column(String)
    region_of_residence = Column(String)
    is_gym_affiliate = Column(String)
    gym_affiliate = Column(String)
    gym_affiliate_location = Column(String)
    is_returning_athlete = Column(Boolean)
    is_returning_athlete_to_city = Column(Boolean)
    is_under_shop = Column(Boolean)
    under_shop_id = Column(String)

class TicketSummary(Base):
    __tablename__ = "ticket_summary"
    
    id = Column(String, primary_key=True)  # Composite of event_id and ticket_type_id
    event_id = Column(String, ForeignKey("events.id"))
    event_name = Column(String)
    ticket_type_id = Column(String)
    ticket_name = Column(String)
    ticket_category = Column(String)
    ticket_event_day = Column(String)
    total_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class SummaryReport(Base):
    __tablename__ = "summary_report"
    
    # Simple auto-incrementing ID
    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(String, nullable=False)
    ticket_group = Column(String, nullable=False)
    total_count = Column(Integer, nullable=False)
    ticket_type_ids = Column(ARRAY(String))
    ticket_names = Column(ARRAY(String))
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())


class TicketUnderShop(Base):
    """Stores information about the underShops in an event"""
    __tablename__ = "ticket_under_shops"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(String, ForeignKey("events.id"), nullable=False)
    shop_id = Column(String, nullable=False)  # _id from underShops
    shop_name = Column(String)  # Trimmed name from underShops
    shop_category = Column(String)  # Derived from customerTags with HTCACCESS or PARTNERACCESS prefix
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class TicketVolumes(Base):
    """Stores volume information for tickets in underShops"""
    __tablename__ = "ticket_volumes"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(String, ForeignKey("events.id"), nullable=False)
    shop_id = Column(String, nullable=False)  # _id from underShops
    ticket_type_id = Column(String, nullable=False)  # baseTicket from underShops.tickets
    volume = Column(Integer)  # amount from underShops.tickets
    ticket_shop_category = Column(String, default='all')  # Based on shop_category
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class TicketUnderShopSummary(Base):
    """Summary of tickets sold through underShops"""
    __tablename__ = "ticket_under_shop_summary"
    
    id = Column(String, primary_key=True)  # Composite of event_id, ticket_type_id, and under_shop_id
    event_id = Column(String, ForeignKey("events.id"), nullable=False)
    event_name = Column(String)
    ticket_type_id = Column(String, nullable=False)
    ticket_name = Column(String)
    ticket_category = Column(String)
    ticket_event_day = Column(String)
    under_shop_id = Column(String, nullable=False)
    shop_category = Column(String)
    ticket_count = Column(Integer, default=0)  # Renamed from total_count
    ticket_volume = Column(Integer, default=0)  # New field for available volume
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

# For backward compatibility
TicketTypeSummary = TicketSummary