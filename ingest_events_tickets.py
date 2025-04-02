import logging
import os
import re
import time
import asyncio
from datetime import datetime
from enum import Enum
from math import ceil
from dataclasses import dataclass
from typing import Dict, Set, List, Tuple, Optional, Union, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

# Third party imports
import requests
import httpx
from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text, func, inspect

# Local imports
from models.database import Base, Event, Ticket, TicketTypeSummary, SummaryReport

# Create logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

class Config:
    """
    Application configuration management with secure handling of secrets.
    
    This class centralizes configuration settings from environment variables,
    provides validation, and offers methods to access configuration data.
    """
    
    def __init__(self):
        """Initialize configuration with defaults and environment variables."""
        # Load environment variables if not already loaded
        load_dotenv()
        
        # API Configuration
        self.api_base_url = os.getenv('EVENT_API_BASE_URL', '')
        self.api_token = os.getenv('EVENT_API_TOKEN', '')
        
        # Database Configuration
        self.db_host = os.getenv('POSTGRES_HOST', 'localhost')
        self.db_port = int(os.getenv('POSTGRES_PORT', '5432'))
        self.db_name = os.getenv('POSTGRES_DB', '')
        self.db_user = os.getenv('POSTGRES_USER', '')
        self.db_password = os.getenv('POSTGRES_PASSWORD', '')
        
        # Application Configuration
        self.debug_mode = os.getenv('DEBUG', 'false').lower() == 'true'
        self.batch_size = int(os.getenv('BATCH_SIZE', '1000'))
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        self.retry_delay = int(os.getenv('RETRY_DELAY', '5'))
        self.enable_file_logging = os.getenv('ENABLE_FILE_LOGGING', 'true').lower() == 'true'
        self.enable_growth_analysis = os.getenv('ENABLE_GROWTH_ANALYSIS', 'false').lower() == 'true'
        
    def validate(self):
        """Validate critical configuration settings."""
        missing = []
        
        if not self.api_base_url:
            missing.append('EVENT_API_BASE_URL')
        if not self.api_token:
            missing.append('EVENT_API_TOKEN')
        if not self.db_name:
            missing.append('POSTGRES_DB')
        if not self.db_user:
            missing.append('POSTGRES_USER')
            
        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")
            
    def get_db_uri(self) -> str:
        """
        Get SQLAlchemy database URI with proper escaping of special characters.
        
        Returns:
            str: Database connection URI
        """
        # Escape special characters in password
        password = self.db_password.replace('%', '%25').replace(':', '%3A').replace('@', '%40')
        
        return f"postgresql://{self.db_user}:{password}@{self.db_host}:{self.db_port}/{self.db_name}"
        
    def is_production(self) -> bool:
        """
        Check if the application is running in production mode.
        
        Returns:
            bool: True if in production mode, False otherwise
        """
        return os.getenv('ENVIRONMENT', 'development').lower() == 'production'
        
    def get_log_level(self) -> int:
        """
        Get the appropriate log level based on debug mode.
        
        Returns:
            int: Logging level constant
        """
        return logging.DEBUG if self.debug_mode else logging.INFO
        
    @classmethod
    def get_event_configs(cls):
        """
        Get all event configurations from environment variables.
        
        Returns:
            List[Dict]: List of configuration dictionaries
        """
        from collections import defaultdict
        
        configs = defaultdict(dict)
        for key, value in os.environ.items():
            if key.startswith("EVENT_CONFIGS__"):
                _, region, param = key.split("__", 2)
                if param in ["token", "event_id", "schema_name"]:
                    configs[region][param] = value
                configs[region]["region"] = region

        return [
            {
                "token": config["token"],
                "event_id": config["event_id"],
                "schema": config["schema_name"],
                "region": config["region"]
            }
            for config in configs.values()
            if all(k in config for k in ["token", "event_id", "schema_name", "region"])
        ]

class LogConfig:
    """
    Configuration and management for application logging.
    
    This class handles setting up logging with console and file handlers,
    and provides methods to dynamically change logging levels.
    """
    
    def __init__(self, config: Config = None):
        """
        Initialize logging configuration.
        
        Args:
            config: Application configuration
        """
        self.config = config or Config()
        self.logger = self._setup_logging()
        
    def _setup_logging(self) -> logging.Logger:
        """
        Set up logging with console and optional file handlers.
        
        Returns:
            logging.Logger: Configured logger instance
        """
        # Create logs directory if it doesn't exist
        os.makedirs('logs', exist_ok=True)
        
        # Get the main logger
        logger = logging.getLogger(__name__)
        logger.setLevel(self.config.get_log_level())
        
        # Clear any existing handlers
        if logger.hasHandlers():
            logger.handlers.clear()
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.config.get_log_level())
        
        # Create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        
        # Add the console handler to the logger
        logger.addHandler(console_handler)
        
        # Check if file logging is enabled
        if self.config.enable_file_logging:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = f'logs/ingest_{timestamp}.log'
            file_handler = logging.FileHandler(log_filename)
            file_handler.setLevel(logging.DEBUG)  # Always debug level for file logs
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
        return logger
        
    def set_debug(self, enabled: bool):
        """
        Enable or disable debug logging.
        
        Args:
            enabled: True to enable debug logging, False to revert to default level
        """
        level = logging.DEBUG if enabled else logging.INFO
        self.logger.setLevel(level)
        
        # Update console handler level
        for handler in self.logger.handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                handler.setLevel(level)
                
        self.debug_enabled = enabled
        logger.info(f"Debug logging {'enabled' if enabled else 'disabled'}")

# Initialize configuration
app_config = Config()

# Set up logging
log_config = LogConfig(app_config)
logger = log_config.logger

class TicketCategory(Enum):
    SINGLE = "single"
    DOUBLES = "double"
    RELAY = "relay"
    SPECTATOR = "spectator"
    EXTRA = "extra"
    
class TicketEventDay(Enum):
    FRIDAY = "friday"
    SATURDAY = "saturday"
    SUNDAY = "sunday"

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

class BaseVivenuAPI:
    """Base class for Vivenu API implementations"""
    
    def __init__(self, token: str):
        """
        Initialize the API with authentication token and common configurations.
        
        Args:
            token: API authentication token
        """
        self.token = token
        self.base_url = os.getenv('EVENT_API_BASE_URL', '').rstrip('/')  # Remove trailing slash if present
        
        # Common headers for all implementations
        self.base_headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        }
        
        # Add browser-like headers to help with Cloudflare
        self.browser_headers = {
            **self.base_headers,
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://vivenu.com",
            "Referer": "https://vivenu.com/"
        }
        
        logger.debug(f"API initialized with URL: {self.base_url}")
    
    def get_events(self):
        """
        Fetch events from the API.
        
        Returns:
            dict: API response with events data
        
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError("Subclasses must implement get_events()")
    
    def get_tickets(self, skip: int = 0, limit: int = 1000):
        """
        Fetch tickets from the API with pagination.
        
        Args:
            skip: Number of tickets to skip (for pagination)
            limit: Maximum number of tickets to return
            
        Returns:
            dict: API response with tickets data
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError("Subclasses must implement get_tickets()")
    
    async def close(self):
        """Clean up resources used by the API client"""
        pass


class VivenuAPI(BaseVivenuAPI):
    """Synchronous API implementation using requests library"""
    
    def __init__(self, token: str):
        """
        Initialize the synchronous API client.
        
        Args:
            token: API authentication token
        """
        super().__init__(token)
        # Use only base headers for simplicity
        self.headers = self.base_headers
        logger.debug(f"Using headers: {self.headers}")
        
        # Create a session for connection pooling and better performance
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def get_events(self):
        """Get events using synchronous requests"""
        logger.debug(f"Making request to: {self.base_url}/events")
        response = self.session.get(f"{self.base_url}/events", verify=False)
        response.raise_for_status()
        return response.json()

    def get_tickets(self, skip: int = 0, limit: int = 1000):
        """Get tickets using synchronous requests with pagination"""
        params = {
            "status": "VALID,DETAILSREQUIRED",
            "skip": skip,
            "top": limit
        }
        logger.debug(f"Making request to: {self.base_url}/tickets with params {params}")
        response = self.session.get(f"{self.base_url}/tickets", params=params, verify=False)
        response.raise_for_status()
        return response.json()
        
    async def close(self):
        """Close the requests session"""
        if hasattr(self, 'session'):
            self.session.close()


class VivenuHttpxAPI(BaseVivenuAPI):
    """Asynchronous API implementation using httpx library"""
    
    def __init__(self, token: str):
        """
        Initialize the asynchronous API client.
        
        Args:
            token: API authentication token
        """
        super().__init__(token)
        # Use browser headers for better Cloudflare handling
        self.headers = self.browser_headers
        logger.debug(f"Using headers: {self.headers}")
        
        self._client = None
        self._loop = None
        
    async def _ensure_client(self):
        """
        Ensure httpx client exists or create a new one.
        
        Returns:
            httpx.AsyncClient: The async HTTP client
        """
        if self._client is None:
            self._client = httpx.AsyncClient(
                headers=self.headers,
                verify=False,  # Disable SSL verification
                timeout=30.0,
                limits=httpx.Limits(
                    max_keepalive_connections=10,
                    max_connections=20,
                    keepalive_expiry=30.0
                )
            )
        return self._client
        
    async def _get_events_async(self):
        """
        Async implementation of get_events using httpx.
        
        Returns:
            dict: API response with events data
            
        Raises:
            Exception: Any error during API request
        """
        client = await self._ensure_client()
        url = f"{self.base_url}/events"
        
        logger.debug(f"Making httpx request to: {url}")
        
        try:
            # Add a small delay to avoid rate limiting
            await asyncio.sleep(0.5)
            
            response = await client.get(url)
            
            if response.status_code != 200:
                logger.error(f"Error response status: {response.status_code}")
                logger.error(f"Error response body: {response.text}")
                logger.error(f"Request headers sent: {client.headers}")
                response.raise_for_status()
                
            return response.json()
        except Exception as e:
            logger.error(f"Httpx request failed: {str(e)}")
            raise
            
    async def _get_tickets_async(self, skip: int = 0, limit: int = 1000):
        """
        Async implementation of get_tickets using httpx.
        
        Args:
            skip: Number of tickets to skip (for pagination)
            limit: Maximum number of tickets to return
            
        Returns:
            dict: API response with tickets data
            
        Raises:
            Exception: Any error during API request
        """
        client = await self._ensure_client()
        url = f"{self.base_url}/tickets"
        
        params = {
            "status": "VALID,DETAILSREQUIRED",
            "skip": skip,
            "top": limit
        }
        
        try:
            # Add a small delay to avoid rate limiting
            await asyncio.sleep(0.5)
            
            response = await client.get(url, params=params)
            
            if response.status_code != 200:
                logger.error(f"Error response status: {response.status_code}")
                logger.error(f"Error response body: {response.text}")
                logger.error(f"Request headers sent: {client.headers}")
                response.raise_for_status()
                
            return response.json()
        except Exception as e:
            logger.error(f"Httpx request failed: {str(e)}")
            raise

    def _get_or_create_loop(self):
        """
        Get existing loop or create a new one if needed.
        
        Returns:
            asyncio.AbstractEventLoop: The event loop
        """
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            return loop
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop

    def _is_loop_running(self):
        """
        Check if an event loop is already running.
        
        Returns:
            bool: True if a loop is running, False otherwise
        """
        try:
            loop = asyncio.get_event_loop()
            return loop.is_running()
        except RuntimeError:
            return False

    def get_events(self):
        """
        Synchronous wrapper for the async method.
        
        Returns:
            dict: API response with events data
        """
        # Check if we're in an already running event loop
        if self._is_loop_running():
            logger.debug("Event loop already running, using direct synchronous HTTP request for events")
            # Use a requests-based approach as a fallback
            headers = self.browser_headers
            try:
                import requests
                response = requests.get(f"{self.base_url}/events", headers=headers, verify=False)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                logger.error(f"Direct synchronous request failed: {str(e)}")
                raise
        
        # Normal async execution path
        loop = self._get_or_create_loop()
        try:
            return loop.run_until_complete(self._get_events_async())
        except RuntimeError as e:
            if str(e) == "Event loop is closed":
                # Create a new loop and try again
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                return loop.run_until_complete(self._get_events_async())
            raise

    def get_tickets(self, skip: int = 0, limit: int = 1000):
        """
        Synchronous wrapper for the async method.
        
        Args:
            skip: Number of tickets to skip (for pagination)
            limit: Maximum number of tickets to return
            
        Returns:
            dict: API response with tickets data
        """
        # Check if we're in an already running event loop
        if self._is_loop_running():
            logger.debug("Event loop already running, using direct synchronous HTTP request for tickets")
            # Use a requests-based approach as a fallback
            headers = self.browser_headers
            params = {
                "status": "VALID,DETAILSREQUIRED",
                "skip": skip,
                "top": limit
            }
            try:
                import requests
                response = requests.get(f"{self.base_url}/tickets", headers=headers, params=params, verify=False)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                logger.error(f"Direct synchronous request failed: {str(e)}")
                raise
        
        # Normal async execution path
        loop = self._get_or_create_loop()
        try:
            return loop.run_until_complete(self._get_tickets_async(skip, limit))
        except RuntimeError as e:
            if str(e) == "Event loop is closed":
                # Create a new loop and try again
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                return loop.run_until_complete(self._get_tickets_async(skip, limit))
            raise
        
    async def close(self):
        """Close the httpx client"""
        if self._client:
            await self._client.aclose()
            self._client = None
            
    def __del__(self):
        """Ensure the client is closed when the object is destroyed"""
        if hasattr(self, '_client') and self._client:
            try:
                loop = self._get_or_create_loop()
                if not loop.is_closed():
                    loop.run_until_complete(self.close())
            except Exception as e:
                logger.debug(f"Error closing httpx client: {str(e)}")

class DatabaseManager:
    """
    Manages database connections, schema setup, and session management.
    
    This class handles creating the database engine, setting up schemas and tables,
    and providing session factory functionality.
    """
    
    def __init__(self, schema: str, config: Config = None):
        """
        Initialize the database manager.
        
        Args:
            schema: Database schema to use
            config: Application configuration
        """
        self.schema = schema
        self.config = config or app_config
        self.engine = self._create_engine()
        self._session_factory = sessionmaker(bind=self.engine)
        
    def _create_engine(self):
        """
        Create SQLAlchemy engine using configuration settings.
        
        Returns:
            sqlalchemy.engine.Engine: Database engine
        
        Raises:
            ValueError: If database configuration is invalid
        """
        try:
            # Validate database configuration
            if not self.config.db_name or not self.config.db_user:
                raise ValueError("Missing database configuration")
                
            db_uri = self.config.get_db_uri()
            logger.debug(f"Creating database engine with URI: {db_uri.replace(self.config.db_password, '******')}")
            
            # Create engine with appropriate settings
            return create_engine(
                db_uri,
                pool_pre_ping=True,  # Check connection validity before using
                pool_size=5,         # Connection pool size
                max_overflow=10,     # Max extra connections when pool is full
                pool_timeout=30,     # Seconds to wait for connection from pool
                pool_recycle=1800    # Recycle connections after 30 minutes
            )
        except Exception as e:
            logger.error(f"Failed to create database engine: {str(e)}")
            raise

    def get_session(self):
        """
        Create a new session and set the schema search path.
        
        Returns:
            sqlalchemy.orm.Session: Database session
        """
        session = self._session_factory()
        session.execute(text(f"SET search_path TO {self.schema}"))
        return session

    def setup_schema(self):
        """
        Set up schema and tables with appropriate error handling.
        
        Raises:
            Exception: If schema setup fails
        """
        try:
            logger.info(f"Setting up schema and tables for {self.schema}")
            
            with self.engine.connect() as conn:
                # Create schema if it doesn't exist
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema}"))
                conn.execute(text(f"SET search_path TO {self.schema}"))
                
                # Drop existing tables with exception for summary_report if growth analysis is enabled
                if not self.config.enable_growth_analysis:
                    conn.execute(text(f"DROP TABLE IF EXISTS {self.schema}.summary_report CASCADE"))
                    
                conn.execute(text(f"""
                    DROP TABLE IF EXISTS {self.schema}.ticket_type_summary CASCADE;
                    DROP TABLE IF EXISTS {self.schema}.tickets CASCADE;
                    DROP TABLE IF EXISTS {self.schema}.events CASCADE;
                """))
                conn.commit()

            # Create tables
            Base.metadata.schema = self.schema
            Base.metadata.create_all(self.engine)
            logger.info(f"Successfully set up schema and tables for {self.schema}")
            
        except Exception as e:
            logger.error(f"Failed to set up schema {self.schema}: {str(e)}")
            raise
            
    def check_connection(self) -> bool:
        """
        Check if database connection is working.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                return True
        except Exception as e:
            logger.error(f"Database connection check failed: {str(e)}")
            return False

class TransactionManager:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def __enter__(self):
        self.session = self.db_manager.get_session()
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                self.session.commit()
            else:
                self.session.rollback()
        finally:
            self.session.close()

def verify_tables(session, schema: str):
    """Verify that tables exist with correct columns"""
    try:
        # Check if event_id column exists
        result = session.execute(text(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = '{schema}' 
            AND table_name = 'tickets' 
            AND column_name = 'event_id'
        """))
        
        if not result.fetchone():
            logger.error(f"event_id column not found in {schema}.tickets")
            raise Exception("Required columns not found")
            
        logger.info(f"Table verification successful for schema {schema}")
    except Exception as e:
        logger.error(f"Table verification failed for schema {schema}: {e}")
        raise

def determine_ticket_group(ticket_name: str) -> TicketCategory:
    """Determine basic ticket group (single, double, relay, spectator, extra)"""
    name_lower = ticket_name.lower()
    if 'friend' in name_lower or 'sportograf' in name_lower or 'transfer' in name_lower or 'complimentary' in name_lower:
        return TicketCategory.EXTRA 
    elif 'double' in name_lower:
        return TicketCategory.DOUBLES
    elif 'relay' in name_lower:
        return TicketCategory.RELAY
    elif 'spectator' in name_lower:
        return TicketCategory.SPECTATOR
    return TicketCategory.SINGLE

def determine_ticket_event_day(ticket_name: str) -> TicketEventDay:
    """Determine basic ticket group (friday, saturday, sunday)"""
    name_lower = ticket_name.lower()
    if 'sunday' in name_lower:
        return TicketEventDay.SUNDAY 
    elif 'saturday' in name_lower:
        return TicketEventDay.SATURDAY
    elif 'friday' in name_lower:
        return TicketEventDay.FRIDAY
    return TicketEventDay.SATURDAY

def calculate_age(birth_date) -> Union[int, None]:
    if birth_date:
        birth_date = datetime.strptime(birth_date, "%Y-%m-%d")
        today = datetime.today()
        return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
    return None

def standardize_gender(gender: str) -> Union[str, None]:
    """Standardize gender input to 'Male' or 'Female'.
    
    Handles:
    1. Single gender strings: 'male', 'men', 'female', 'woman', 'women'
    2. Multi-language gender strings where English appears first:
       - "Female เพศหญิง" (Thai)
       - "Male 남성" (Korean)
       - "Female 女性" (Japanese/Chinese)
    """
    if not gender:
        return None
        
    # Get first word and normalize
    first_word = str(gender).split()[0].lower().strip()
    
    if first_word in ['male', 'men']:
        return 'Male'
    elif first_word in ['female', 'woman', 'women']:
        return 'Female'
    
    return None

def parse_datetime(dt_str):
    """Parse datetime string to datetime object"""
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        return None

def update_ticket_summary(session, schema: str, event_id: str):
    """Update ticket type summary for an event"""
    try:
        # Get event details first
        event = session.query(Event).filter(Event.id == event_id).first()
        if not event:
            logger.warning(f"Event {event_id} not found for summary update")
            return

        # Create a lookup dictionary for ticket names using ticket_type_id
        ticket_name_map = {ticket.get('id'): ticket.get('name') for ticket in event.tickets}
        
        # Get ticket counts grouped by type and category
        ticket_counts = (
            session.query(
                Ticket.event_id,
                Ticket.ticket_type_id,
                func.count().label('total_count')
            )
            .filter(Ticket.event_id == event_id)
            .group_by(
                Ticket.event_id,
                Ticket.ticket_type_id
            )
            .all()
        )

        # Update summary records
        for count in ticket_counts:
            ticket_name = ticket_name_map.get(count.ticket_type_id, '')
            summary_id = f"{count.event_id}_{count.ticket_type_id}"
            
            summary = session.get(TicketTypeSummary, summary_id)
            if summary:
                summary.total_count = count.total_count
            else:
                summary = TicketTypeSummary(
                    id=summary_id,
                    event_id=count.event_id,
                    event_name=event.name,
                    ticket_type_id=count.ticket_type_id,
                    ticket_name=ticket_name,
                    ticket_category=determine_ticket_group(ticket_name).value,
                    ticket_event_day=determine_ticket_event_day(ticket_name).value,
                    total_count=count.total_count
                )
                session.add(summary)

        session.commit()
        

        logger.info(f"Updated ticket summary for event: {event_id} in schema: {schema}")

    except Exception as e:
        session.rollback()
        logger.error(f"Error updating ticket summary in schema {schema}: {e}")
        raise

def get_ticket_summary(session, schema: str, event_id: str) -> Dict[str, SummaryReport]:
    """Get the summarized ticket counts for the event."""
    try:
        # Query to get the total counts for each ticket group and collect ticket info
        with open('sql/get_summary_report.sql', 'r') as file:
            sql_template = file.read()
        query = text(sql_template.replace('{SCHEMA}', schema))
        results = session.execute(query, {"event_id": event_id}).fetchall()
        
        # Convert results to a dictionary with additional info
        summary_data = {}
        for row in results:
            summary_data[row[0]] = {
                'total_count': row[3],
                'ticket_type_ids': row[1],
                'ticket_names': row[2]
            }
        
        return summary_data

    except Exception as e:
        logger.error(f"Error getting ticket summary: {e}")
        return {}

def update_summary_report(session, schema: str, event_id: str):
    """Update summary report with current ticket counts while preserving history"""
    try:
        logger.info(f"Updating summary report for event {event_id} in schema {schema}")
        current_time = datetime.now()
        summary_data = get_ticket_summary(session, schema, event_id)
        
        for ticket_group, data in summary_data.items():
            logger.info(f"Inserting summary for ticket group: {ticket_group}")
            summary = SummaryReport(
                event_id=event_id,
                ticket_group=ticket_group,
                total_count=data.get('total_count', 0),
                ticket_type_ids=data.get('ticket_type_ids'),
                ticket_names=data.get('ticket_names'),
                created_at=current_time,
                updated_at=current_time
            )
            session.add(summary)
        
        session.commit()
        logger.info(f"Summary report updated for event {event_id}")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error updating summary report: {e}")
        raise

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
        groups=event_data.get('groups', []),
        tickets=[{'id': ticket['_id'], 'name': ticket['name']} for ticket in event_data.get('tickets', [])]
    )
    
    try:
        session.merge(event)
        session.commit()
        return event
    except Exception as e:
        session.rollback()
        logger.error(f"Error creating event: {e}")
        raise

class CustomFieldMapper:
    """Manages custom field mappings for different regions/schemas"""
    
    def __init__(self, schema: str, region: str):
        self.schema = schema
        self.region = region
        self._field_mappings = self._load_field_mappings()

    def _load_field_mappings(self) -> Dict[str, str]:
        """
        Dynamically load all field mappings from environment variables
        Format: EVENT_CONFIGS__{region}__field_{database_column}={api_field_name}
        """
        mappings = {}
        prefix = f'EVENT_CONFIGS__{self.region}__field_'
        
        # Scan all environment variables for field mappings
        for key, value in os.environ.items():
            if key.startswith(prefix):
                # Convert environment key to database column name
                db_column = key[len(prefix):].lower()
                mappings[db_column] = value
        
        return mappings

    def get_field_value(self, extra_fields: Dict[str, Any], db_column: str) -> Optional[Any]:
        """Get value from extra_fields using the mapped API field name"""
        api_field = self._field_mappings.get(db_column)
        if not api_field:
            return None
        value = extra_fields.get(api_field)
        return value

    def normalize_value(self, value: Optional[str]) -> Optional[str]:
        """
        Normalize string values by trimming whitespace, removing special characters,
        and handling invalid/empty/NA variations.
        
        Args:
            value: Input string to normalize
            
        Returns:
            Normalized string or None if value is empty/invalid/NA
        """
        if not value:
            return None
        
        # Remove special characters and extra spaces
        normalized = re.sub(r'[^a-zA-Z0-9\s]', '', str(value))
        normalized = ' '.join(normalized.split())  # Handle multiple spaces
        normalized = normalized.strip()
        
        # Return None for empty, single character, or invalid values
        if (not normalized or                          # Empty string
            len(normalized) <= 1 or                    # Single character
            normalized.lower() in [                    # Invalid values
                'na', 'n/a', 'none', 'no', 
                'nil', 'other', ''
            ]):
            return None
        
        return normalized

    def get_gym_affiliate(self, extra_fields: Dict[str, Any]) -> Optional[str]:
        """
        Determine gym affiliate based on membership status and region.
        
        Args:
            extra_fields: Dictionary containing ticket extra fields
            
        Returns:
            Normalized gym affiliate value or None
        """
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
        """
        Determine gym affiliate location based on membership status
        
        Args:
            extra_fields: Dictionary containing ticket extra fields
            
        Returns:
            Resolved gym affiliate location value or None
        """
        # Get membership status
        is_gym_affiliate_condition = extra_fields.get("hyrox_training_clubs")
        membership_status = GymMembershipStatus.parse(is_gym_affiliate_condition)
        
        if not membership_status:
            return None
            
        if membership_status == GymMembershipStatus.MEMBER_OTHER:
            return extra_fields.get('region_training')
        elif membership_status == GymMembershipStatus.MEMBER:
            return extra_fields.get('local_territory_training')
        
        return None

class TicketProcessor:
    """Efficient ticket processing with lookup caching and validation"""
    
    def __init__(self, session, schema, region):
        self.session = session
        self.schema = schema
        self.existing_tickets_cache = {}
        self.field_mapper = CustomFieldMapper(schema, region)
    
    def get_existing_ticket(self, ticket_id: str) -> Optional[Ticket]:
        """Efficiently lookup ticket from cache or database"""
        cache_key = f"{ticket_id}_{self.schema}"
        if cache_key not in self.existing_tickets_cache:
            ticket = self.session.query(Ticket).filter(
                Ticket.id == ticket_id,
                Ticket.region_schema == self.schema
            ).first()
            self.existing_tickets_cache[cache_key] = ticket
        return self.existing_tickets_cache[cache_key]
    
    def process_ticket(self, ticket_data: Dict, event_data: Dict) -> Optional[Ticket]:
        """Process single ticket with validation and efficient lookup"""
        ticket_id = str(ticket_data.get("_id"))
        if not ticket_id:
            logger.warning("Ticket ID is missing, skipping")
            return None

        try:
            # Basic validations
            event_id = event_data.get("_id")
            if event_id != ticket_data.get("eventId"):
                logger.debug(f"Skipping ticket {ticket_id} - event ID mismatch")
                return None

            ticket_name = ticket_data.get("ticketName")
            if not ticket_name:
                logger.debug(f"Ticket name is missing for ticket ID {ticket_id}")
                return None

            # Get extra fields
            extra_fields = ticket_data.get("extraFields", {})

            # Prepare ticket values
            ticket_values = {
                'id': ticket_id,
                'region_schema': self.schema,
                'transaction_id': ticket_data.get("transactionId"),
                'ticket_type_id': ticket_data.get("ticketTypeId"),
                'currency': ticket_data.get("currency"),
                'status': ticket_data.get("status"),
                'personalized': ticket_data.get("personalized", False),
                'expired': ticket_data.get("expired", False),
                'event_id': event_id,
                'ticket_name': ticket_name,
                'category_name': ticket_data.get("categoryName"),
                'barcode': ticket_data.get("barcode"),
                'created_at': ticket_data.get("createdAt"),
                'updated_at': ticket_data.get("updatedAt"),
                'city': ticket_data.get("city"),
                'country': ticket_data.get("country"),
                'customer_id': ticket_data.get("customerId"),
                'gender': standardize_gender(extra_fields.get("gender")),
                'birthday': extra_fields.get("birth_date"),
                'age': calculate_age(extra_fields.get("birth_date")),
                'nationality': extra_fields.get("nationality"),
                'region_of_residence': extra_fields.get("region_of_residence"),
                'is_gym_affiliate': extra_fields.get("hyrox_training_clubs"),
                'gym_affiliate': self.field_mapper.get_gym_affiliate(extra_fields),
                'gym_affiliate_location': self.field_mapper.get_gym_affiliate_location(extra_fields),
                'is_returning_athlete': normalize_yes_no(extra_fields.get("returning_athlete")),
                'is_returning_athlete_to_city': normalize_yes_no(extra_fields.get("returning_athlete_city"))
            }

            # Update or create ticket
            existing_ticket = self.get_existing_ticket(ticket_id)
            if existing_ticket:
                for key, value in ticket_values.items():
                    setattr(existing_ticket, key, value)
                return existing_ticket
            else:
                new_ticket = Ticket(**ticket_values)
                self.session.add(new_ticket)
                self.existing_tickets_cache[f"{ticket_id}_{self.schema}"] = new_ticket
                return new_ticket

        except Exception as e:
            logger.error(f"Error processing ticket {ticket_id}: {str(e)}")
            return None

    def clear_cache(self):
        """Clear the lookup cache"""
        self.existing_tickets_cache.clear()

def process_batch(session, tickets: List, event_data: Dict, schema: str, region: str):
    """Process batch of tickets using TicketProcessor"""
    processed = 0
    failed = 0
    
    processor = TicketProcessor(session, schema, region)
    
    for ticket in tickets:
        try:
            result = processor.process_ticket(ticket, event_data)
            if result:
                processed += 1
        except Exception as e:
            failed += 1
            logger.error(f"Failed to process ticket {ticket.get('_id')}: {str(e)}")
            continue
    
    processor.clear_cache()
    logger.info(f"Batch summary - Processed: {processed}, Failed: {failed}")
    return processed

class BatchProcessor:
    """
    Process data in batches with optimized performance and error handling.
    
    This class handles batched processing of tickets with various API implementations,
    managing resources efficiently and providing robust error handling.
    """
    
    def __init__(self, api, batch_size=1000, max_retries=3, retry_delay=5):
        """
        Initialize the batch processor.
        
        Args:
            api: API implementation (VivenuAPI or VivenuHttpxAPI)
            batch_size: Number of items to process in each batch
            max_retries: Maximum number of retries for failed requests
            retry_delay: Delay in seconds between retries
        """
        self.api = api
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay
    
    def process_tickets_sync(self, tickets_processor_fn, start_from=0):
        """
        Synchronous wrapper for process_tickets async method.
        
        Args:
            tickets_processor_fn: Function to process each batch of tickets
            start_from: Starting index for ticket pagination
            
        Returns:
            tuple: (total_tickets_processed, has_more)
        """
        # Check if we're already in an event loop
        try:
            loop = asyncio.get_event_loop()
            in_event_loop = loop.is_running()
        except RuntimeError:
            in_event_loop = False
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
        # If we're already in an event loop, we need to handle it differently
        if in_event_loop:
            logger.info("Using direct synchronous processing for tickets (event loop already running)")
            return self._process_tickets_sync_direct(tickets_processor_fn, start_from)
        else:
            # Not in event loop, so we can use run_until_complete
            logger.info("Using async processing with run_until_complete for tickets")
            try:
                return loop.run_until_complete(self.process_tickets(tickets_processor_fn, start_from))
            except Exception as e:
                logger.error(f"Error in process_tickets_sync: {str(e)}")
                raise
    
    def _process_tickets_sync_direct(self, tickets_processor_fn, start_from=0):
        """
        Direct synchronous implementation of ticket processing.
        Used when we're already in an event loop.
        
        Args:
            tickets_processor_fn: Function to process each batch of tickets
            start_from: Starting index for ticket pagination
            
        Returns:
            tuple: (total_tickets_processed, has_more)
        """
        # Track metrics
        total_processed = 0
        batch_count = 0
        has_more = True
        retry_count = 0
        current_skip = start_from
        
        try:
            while has_more and retry_count < self.max_retries:
                try:
                    batch_start_time = time.time()
                    logger.info(f"Fetching batch {batch_count+1} (skip={current_skip}, limit={self.batch_size})")
                    
                    # Get tickets for this batch (synchronously)
                    response = self.api.get_tickets(skip=current_skip, limit=self.batch_size)
                        
                    # Process the response
                    if not response:
                        logger.warning("Empty API response")
                        has_more = False
                        break
                    
                    # Check for tickets in response - handle different API response formats
                    tickets = None
                    if 'value' in response:
                        tickets = response.get('value', [])
                    elif 'rows' in response:
                        tickets = response.get('rows', [])
                    else:
                        logger.warning(f"Unexpected API response format: {response}")
                        has_more = False
                        break
                    
                    batch_size = len(tickets)
                    
                    if batch_size == 0:
                        logger.info("No more tickets to process")
                        has_more = False
                        break
                        
                    # Process this batch of tickets
                    logger.info(f"Processing {batch_size} tickets")
                    tickets_processor_fn(tickets)
                    
                    # Update metrics
                    batch_count += 1
                    total_processed += batch_size
                    current_skip += batch_size
                    
                    # Check if we need to continue
                    has_more = batch_size >= self.batch_size
                    
                    # Reset retry counter after successful batch
                    retry_count = 0
                    
                    batch_end_time = time.time()
                    logger.info(f"Batch {batch_count} completed in {batch_end_time - batch_start_time:.2f} seconds")
                    
                    # Add a small delay between batches to avoid rate limiting
                    time.sleep(1)
                        
                except Exception as e:
                    logger.error(f"Error processing batch: {str(e)}")
                    retry_count += 1
                    
                    if retry_count >= self.max_retries:
                        logger.error(f"Max retries ({self.max_retries}) reached, stopping batch processing")
                        break
                        
                    # Exponential backoff for retries
                    backoff_time = self.retry_delay * (2 ** (retry_count - 1))
                    logger.info(f"Retry {retry_count}/{self.max_retries} in {backoff_time} seconds")
                    time.sleep(backoff_time)
            
            logger.info(f"Direct batch processing completed: {total_processed} tickets in {batch_count} batches")
            return total_processed, has_more
            
        finally:
            # Clean up resources
            if hasattr(self.api, 'close') and not asyncio.iscoroutinefunction(self.api.close):
                self.api.close()

    async def process_tickets(self, tickets_processor_fn, start_from=0):
        """
        Process tickets in batches using the provided processor function.
        
        Args:
            tickets_processor_fn: Function to process each batch of tickets
            start_from: Starting index for ticket pagination
            
        Returns:
            tuple: (total_tickets_processed, has_more)
        """
        logger.info(f"Starting batch processing from index {start_from} with batch size {self.batch_size}")
        
        # Check if we're already in an event loop
        in_event_loop = False
        try:
            loop = asyncio.get_event_loop()
            in_event_loop = loop.is_running()
        except RuntimeError:
            # No event loop exists yet
            pass

        # If we're already in an event loop and using async API, we can't use await
        # Fall back to direct synchronous processing
        if in_event_loop and asyncio.iscoroutinefunction(self.api.get_tickets):
            logger.info("Event loop already running and async API detected - using direct synchronous processing for tickets")
            return self._process_tickets_sync_direct(tickets_processor_fn, start_from)
        
        # Track metrics
        total_processed = 0
        batch_count = 0
        has_more = True
        retry_count = 0
        
        try:
            # Create a fresh event loop for processing if needed
            using_async_io = False
            
            if asyncio.iscoroutinefunction(self.api.get_tickets):
                using_async_io = True
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_closed():
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
            
            current_skip = start_from
            
            while has_more and retry_count < self.max_retries:
                try:
                    batch_start_time = time.time()
                    logger.info(f"Fetching batch {batch_count+1} (skip={current_skip}, limit={self.batch_size})")
                    
                    # Get tickets for this batch
                    try:
                        if using_async_io:
                            # Use asyncio for the API call
                            response = await self.api.get_tickets(skip=current_skip, limit=self.batch_size)
                        else:
                            # Use synchronous API call
                            response = self.api.get_tickets(skip=current_skip, limit=self.batch_size)
                    except Exception as e:
                        logger.error(f"Error fetching tickets: {str(e)}")
                        raise
                        
                    # Process the response
                    if not response:
                        logger.warning("Empty API response")
                        has_more = False
                        break
                    
                    # Check for tickets in response - handle different API response formats
                    tickets = None
                    if 'value' in response:
                        tickets = response.get('value', [])
                    elif 'rows' in response:
                        tickets = response.get('rows', [])
                    else:
                        logger.warning(f"Unexpected API response format: {response}")
                        has_more = False
                        break
                    
                    batch_size = len(tickets)
                    
                    if batch_size == 0:
                        logger.info("No more tickets to process")
                        has_more = False
                        break
                        
                    # Process this batch of tickets
                    logger.info(f"Processing {batch_size} tickets")
                    tickets_processor_fn(tickets)
                    
                    # Update metrics
                    batch_count += 1
                    total_processed += batch_size
                    current_skip += batch_size
                    
                    # Check if we need to continue
                    has_more = batch_size >= self.batch_size
                    
                    # Reset retry counter after successful batch
                    retry_count = 0
                    
                    batch_end_time = time.time()
                    logger.info(f"Batch {batch_count} completed in {batch_end_time - batch_start_time:.2f} seconds")
                    
                    # Add a small delay between batches to avoid rate limiting
                    if has_more:
                        await asyncio.sleep(1) if using_async_io else time.sleep(1)
                        
                except Exception as e:
                    logger.error(f"Error processing batch: {str(e)}")
                    retry_count += 1
                    
                    if retry_count >= self.max_retries:
                        logger.error(f"Max retries ({self.max_retries}) reached, stopping batch processing")
                        break
                        
                    # Exponential backoff for retries
                    backoff_time = self.retry_delay * (2 ** (retry_count - 1))
                    logger.info(f"Retry {retry_count}/{self.max_retries} in {backoff_time} seconds")
                    await asyncio.sleep(backoff_time) if using_async_io else time.sleep(backoff_time)
            
            logger.info(f"Batch processing completed: {total_processed} tickets in {batch_count} batches")
            return total_processed, has_more
            
        finally:
            # Clean up resources
            try:
                if hasattr(self.api, 'close'):
                    await self.api.close()
            except Exception as e:
                logger.error(f"Error closing API resources: {str(e)}")
                
    def process_events_sync(self, events_processor_fn):
        """
        Synchronous wrapper for process_events async method.
        
        Args:
            events_processor_fn: Function to process the events
            
        Returns:
            int: Number of events processed
        """
        # Check if we're already in an event loop
        try:
            loop = asyncio.get_event_loop()
            in_event_loop = loop.is_running()
        except RuntimeError:
            in_event_loop = False
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
        # If we're already in an event loop, we need to handle it differently
        if in_event_loop:
            logger.info("Using direct synchronous processing for events (event loop already running)")
            return self._process_events_sync_direct(events_processor_fn)
        else:
            # Not in event loop, so we can use run_until_complete
            logger.info("Using async processing with run_until_complete for events")
            try:
                return loop.run_until_complete(self.process_events(events_processor_fn))
            except Exception as e:
                logger.error(f"Error in process_events_sync: {str(e)}")
                raise
                
    def _process_events_sync_direct(self, events_processor_fn):
        """
        Direct synchronous implementation of event processing.
        Used when we're already in an event loop.
        
        Args:
            events_processor_fn: Function to process the events
            
        Returns:
            int: Number of events processed
        """
        try:
            # Get events synchronously
            response = self.api.get_events()
                
            # Process the response - handle different API response formats
            events = None
            if 'value' in response:
                events = response.get('value', [])
            elif 'rows' in response:
                events = response.get('rows', [])
            else:
                logger.warning(f"Unexpected API response format: {response}")
                return 0
                
            event_count = len(events)
            
            if event_count == 0:
                logger.info("No events to process")
                return 0
                
            # Process the events
            logger.info(f"Processing {event_count} events")
            events_processor_fn(events)
            
            logger.info(f"Event processing completed: {event_count} events")
            return event_count
            
        except Exception as e:
            logger.error(f"Error in direct event processing: {str(e)}")
            return 0
        finally:
            # Clean up resources
            if hasattr(self.api, 'close') and not asyncio.iscoroutinefunction(self.api.close):
                self.api.close()

    async def process_events(self, events_processor_fn):
        """
        Process all events using the provided processor function.
        
        Args:
            events_processor_fn: Function to process the events
            
        Returns:
            int: Number of events processed
        """
        logger.info("Processing events")
        
        # Check if we're already in an event loop
        in_event_loop = False
        try:
            loop = asyncio.get_event_loop()
            in_event_loop = loop.is_running()
        except RuntimeError:
            # No event loop exists yet
            pass

        # If we're already in an event loop and using async API, we can't use await
        # Fall back to direct synchronous processing
        if in_event_loop and asyncio.iscoroutinefunction(self.api.get_events):
            logger.info("Event loop already running and async API detected - using direct synchronous processing for events")
            return self._process_events_sync_direct(events_processor_fn)
        
        try:
            # Determine if we're using async API
            using_async_io = asyncio.iscoroutinefunction(self.api.get_events)
            
            # Get all events
            try:
                if using_async_io:
                    # Use asyncio for the API call
                    response = await self.api.get_events()
                else:
                    # Use synchronous API call
                    response = self.api.get_events()
            except Exception as e:
                logger.error(f"Error fetching events: {str(e)}")
                return 0
                
            # Process the response - handle different API response formats
            events = None
            if 'value' in response:
                events = response.get('value', [])
            elif 'rows' in response:
                events = response.get('rows', [])
            else:
                logger.warning(f"Unexpected API response format: {response}")
                return 0
                
            event_count = len(events)
            
            if event_count == 0:
                logger.info("No events to process")
                return 0
                
            # Process the events
            logger.info(f"Processing {event_count} events")
            events_processor_fn(events)
            
            logger.info(f"Event processing completed: {event_count} events")
            return event_count
            
        finally:
            # Clean up resources
            try:
                if hasattr(self.api, 'close'):
                    await self.api.close()
            except Exception as e:
                logger.error(f"Error closing API resources: {str(e)}")
                
        return 0

def ingest_data(token: str, event_id: str, schema: str, region: str, skip_fetch: bool = False, debug: bool = False):
    """
    Main ingestion function that orchestrates the data ingestion process.
    
    Args:
        token: API authentication token
        event_id: Event ID to process
        schema: Database schema name
        region: Region code
        skip_fetch: If True, skip API fetching and only update summaries
        debug: Enable debug logging
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Set up logging level based on debug flag
    log_config.set_debug(debug)
    
    # Create a configuration object specifically for this ingestion
    config = Config()
    config.api_token = token
    
    # Create the database manager
    db_manager = DatabaseManager(schema, config)
    
    # Validate database connection
    if not db_manager.check_connection():
        logger.error(f"Cannot proceed - database connection failed for schema {schema}")
        return False
    
    # Track timing for performance monitoring
    start_time = time.time()
    api = None
    api_type = None
    
    try:
        logger.info(f"Starting data ingestion for event {event_id} in schema {schema}")
        
        # If skip_fetch, just update summaries
        if skip_fetch:
            logger.info("Skipping API fetch, updating summaries only")
            with TransactionManager(db_manager) as session:
                update_ticket_summary(session, schema, event_id)
                update_summary_report(session, schema, event_id)
            
            elapsed = time.time() - start_time
            logger.info(f"Summary updates completed in {elapsed:.2f} seconds")
            return True

        # Set up schema and tables
        db_manager.setup_schema()

        # Initialize API client with fallback strategy
        api, events, api_type = initialize_api(token)
        
        if not api or not events:
            logger.error("Failed to initialize API client or get events data")
            return False
            
        # Find the target event
        found_event_data = find_event(events, event_id)
        if not found_event_data:
            logger.error(f"Event {event_id} not found in API response")
            return False

        # Create the event record in the database
        with TransactionManager(db_manager) as session:
            verify_tables(session, schema)
            event = create_event(session, found_event_data, schema)
            logger.info(f"Created/updated event record: {event.id} - {event.name}")

        # Process tickets with optimized batching
        with TransactionManager(db_manager) as session:
            batch_processor = BatchProcessor(api, batch_size=config.batch_size, max_retries=config.max_retries, retry_delay=config.retry_delay)
            
            # Define a processor function to handle each batch of tickets
            def process_batch_with_session(tickets):
                return process_batch(session, tickets, found_event_data, schema, region)
                
            # Process all tickets using the synchronous wrapper method
            processed_count, has_more = batch_processor.process_tickets_sync(process_batch_with_session, start_from=0)
            
            logger.info(f"Processed {processed_count} tickets for event {event_id}")
        
        # Update summaries in final transaction
        if processed_count > 0:
            with TransactionManager(db_manager) as session:
                update_ticket_summary(session, schema, event_id)
                update_summary_report(session, schema, event_id)
                
            logger.info(f"Successfully processed {processed_count} tickets for event {event_id}")
            
        elapsed = time.time() - start_time
        logger.info(f"Ingestion completed in {elapsed:.2f} seconds")
        return True

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"Error during ingestion for schema {schema} after {elapsed:.2f} seconds: {str(e)}", exc_info=True)
        return False
    finally:
        # Ensure the API session is properly closed
        if api:  # Add check to avoid UnboundLocalError
            cleanup_api_resources(api, api_type)

def initialize_api(token: str) -> Tuple[Optional[BaseVivenuAPI], Optional[Dict], Optional[str]]:
    """
    Initialize API client with fallback strategy.
    
    Args:
        token: API authentication token
        
    Returns:
        tuple: (api_client, events_data, api_type)
    """
    api = None
    events = None
    api_type = None
    
    # Start with httpx implementation
    try:
        logger.info("Using httpx implementation for API access")
        api = VivenuHttpxAPI(token)
        events = api.get_events()
        logger.info("Successfully connected with httpx implementation")
        api_type = "httpx"
        return api, events, api_type
    except Exception as e:
        logger.warning(f"httpx implementation failed: {str(e)}")
        # Clean up httpx client
        cleanup_api_resources(api, "httpx")
    
    # Fall back to standard API
    try:
        logger.info("Falling back to standard requests implementation")
        api = VivenuAPI(token)
        events = api.get_events()
        logger.info("Successfully connected with standard requests implementation")
        api_type = "requests"
        return api, events, api_type
    except Exception as e:
        logger.error(f"All API implementations failed. Last error: {str(e)}")
        return None, None, None

def cleanup_api_resources(api: Optional[BaseVivenuAPI], api_type: Optional[str]):
    """
    Safely clean up API resources.
    
    Args:
        api: API client to clean up
        api_type: Type of API client
    """
    if not api:
        return
        
    try:
        if hasattr(api, 'close'):
            if api_type == "httpx":
                try:
                    # Create a new event loop for cleanup to avoid "loop is closed" errors
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(api.close())
                except Exception as e:
                    logger.debug(f"Error closing httpx client: {str(e)}")
            else:
                # Synchronous close
                if asyncio.iscoroutinefunction(api.close):
                    # Handle async close method in sync context
                    try:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        loop.run_until_complete(api.close())
                    except Exception as e:
                        logger.debug(f"Error closing API client: {str(e)}")
                else:
                    api.close()
    except Exception as e:
        logger.debug(f"Error during API cleanup: {str(e)}")

def find_event(events: Dict, event_id: str) -> Optional[Dict]:
    """
    Find an event by ID in the events data.
    
    Args:
        events: Events data from API
        event_id: Event ID to find
        
    Returns:
        dict: Event data or None if not found
    """
    if not events:
        return None
        
    # Check for events in different possible response formats
    event_list = []
    if "rows" in events:
        event_list = events["rows"]
    elif "value" in events:
        event_list = events["value"]
    else:
        logger.warning(f"Unknown events data format: {list(events.keys())}")
        return None
        
    for event_data in event_list:
        if event_data.get("_id") == event_id:
            logger.info(f"Found matching event: {event_id}")
            return event_data
            
    return None

def get_event_configs():
    """Get all event configurations from environment"""
    from collections import defaultdict
    
    configs = defaultdict(dict)
    for key, value in os.environ.items():
        if key.startswith("EVENT_CONFIGS__"):
            _, region, param = key.split("__", 2)
            if param in ["token", "event_id", "schema_name"]:
                configs[region][param] = value
            configs[region]["region"] = region

    return [
        {
            "token": config["token"],
            "event_id": config["event_id"],
            "schema": config["schema_name"],
            "region": config["region"]
        }
        for config in configs.values()
        if all(k in config for k in ["token", "event_id", "schema_name", "region"])
    ]

def normalize_yes_no(value: Optional[str]) -> Optional[bool]:
    """Normalize Yes/No values to boolean, handling any language
    
    Args:
        value: Input string that starts with Yes/No followed by optional translation
        
    Returns:
        bool: True for strings starting with "Yes", False for strings starting with "No", 
              None for invalid/empty values
    """
    if not value:
        return None
        
    # Get first word (always English Yes/No)
    first_word = str(value).split()[0].lower().strip()
    
    if first_word == 'yes':
        return True
    elif first_word == 'no':
        return False
    
    return None

if __name__ == "__main__":
    """Main execution entry point"""
    import sys  # Make sure we import sys
    
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(
        description='Ingest events and tickets data from the Vivenu API and store them in the database',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--skip-fetch', action='store_true', help='Skip API fetching and only update summaries')
    parser.add_argument('--event-id', type=str, help='Event ID to process (overrides env config)')
    parser.add_argument('--schema', type=str, help='Database schema name (overrides env config)')
    parser.add_argument('--region', type=str, help='Region code (overrides env config)')
    parser.add_argument('--token', type=str, help='API token (overrides env config)')
    args = parser.parse_args()
    
    # Set debug logging if requested
    if args.debug:
        log_config.set_debug(True)
        logger.info("Debug logging enabled")

    # DO NOT initialize a global event loop here - let the functions manage loops as needed
    
    # Process specific event if provided in command line
    if all([args.event_id, args.schema, args.region, args.token]):
        logger.info(f"Processing specific event from command line: {args.event_id}")
        try:
            success = ingest_data(
                token=args.token,
                event_id=args.event_id,
                schema=args.schema,
                region=args.region,
                skip_fetch=args.skip_fetch,
                debug=args.debug
            )
            if not success:
                logger.error(f"Failed to process event {args.event_id}")
                sys.exit(1)
        except Exception as e:
            logger.error(f"Error processing event {args.event_id}: {e}", exc_info=True)
            sys.exit(1)
    else:
        # Get configs from environment
        configs = Config.get_event_configs()
        if not configs:
            logger.error("No valid event configurations found in environment")
            sys.exit(1)
        
        logger.info(f"Found {len(configs)} event configurations to process")
        
        # Track success/failure counts
        success_count = 0
        failure_count = 0
            
        # Process each config
        for config in configs:
            try:
                logger.info(f"Processing schema: {config['schema']}")
                success = ingest_data(
                    token=config["token"], 
                    event_id=config["event_id"], 
                    schema=config["schema"], 
                    region=config["region"],
                    skip_fetch=args.skip_fetch,
                    debug=args.debug
                )
                if success:
                    success_count += 1
                else:
                    failure_count += 1
                    logger.error(f"Failed to process schema {config['schema']}")
            except Exception as e:
                failure_count += 1
                logger.error(f"Failed to process schema {config['schema']}: {e}", exc_info=True)
                continue
                
        # Log summary statistics
        logger.info(f"Processing completed: {success_count} succeeded, {failure_count} failed")
        if failure_count > 0:
            logger.warning("Some events failed to process, check logs for details")
            sys.exit(1)
    
    # No need to clean up any event loops here since we're not creating any at the global level
    logger.info("Processing completed successfully")
    sys.exit(0) 