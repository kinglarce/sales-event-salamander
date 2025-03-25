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

class TicketTypeSummary(Base):
    __tablename__ = "ticket_type_summary"
    
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
    
    __table_args__ = (
        UniqueConstraint('event_id', 'ticket_group', 'created_at', name='unique_summary_timestamp'),
    )