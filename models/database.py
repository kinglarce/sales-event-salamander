from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, JSON, Boolean, UniqueConstraint, MetaData, Float
from sqlalchemy.ext.declarative import declarative_base

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
    categories = Column(JSON)
    tickets = Column(JSON)
    is_active = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, onupdate=datetime.utcnow)

class Ticket(Base):
    __tablename__ = "tickets"
    
    id = Column(String, primary_key=True)
    region_schema = Column(String, nullable=False)
    name = Column(String)
    transaction_id = Column(String)
    ticket_type_id = Column(String)
    currency = Column(String)
    status = Column(String)
    personalized = Column(Boolean)
    expired = Column(Boolean)
    event_id = Column(String, ForeignKey("events.id"))
    seller_id = Column(String)
    ticket_name = Column(String)
    category_name = Column(String)
    real_price = Column(Integer)
    regular_price = Column(Integer)
    barcode = Column(String, unique=True)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    city = Column(String)
    country = Column(String)
    customer_id = Column(String)
    email = Column(String)
    firstname = Column(String)
    lastname = Column(String)
    postal = Column(String)
    ticket_category = Column(String)  # 'regular' or 'spectator'

class TicketTypeSummary(Base):
    __tablename__ = "ticket_type_summary"
    
    id = Column(String, primary_key=True)  # Composite of event_id and ticket_type_id
    event_id = Column(String, ForeignKey("events.id"))
    event_name = Column(String)
    ticket_type_id = Column(String)
    ticket_name = Column(String)
    ticket_category = Column(String)
    group_name = Column(String)
    total_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class SummaryReport(Base):
    __tablename__ = "summary_report"
    
    id = Column(String, primary_key=True) 
    event_id = Column(String, ForeignKey("events.id"))
    ticket_type_ids = Column(JSON)  # Store all related ticket type IDs (source + target)
    ticket_names = Column(JSON)  
    ticket_group = Column(String)  # 'single', 'double', or 'relay'
    total_count = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)