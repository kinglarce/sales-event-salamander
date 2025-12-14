"""
Configuration Management
Centralized configuration with validation and environment variable handling.
"""

import os
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
from dotenv import load_dotenv
import json

logger = logging.getLogger(__name__)


class Environment(Enum):
    """Environment types"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


@dataclass
class DatabaseConfig:
    """Database configuration"""
    host: str
    port: int
    database: str
    username: str
    password: str
    schema: str
    
    @property
    def connection_url(self) -> str:
        """Get database connection URL"""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class EventConfig:
    """Event configuration"""
    token: str
    event_id: str
    schema: str
    region: str
    base_url: str


@dataclass
class LoggingConfig:
    """Logging configuration"""
    level: str = "INFO"
    enable_file_logging: bool = True
    log_directory: str = "logs"
    max_file_size: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 5


@dataclass
class HTTPConfig:
    """HTTP client configuration"""
    timeout: float = 30.0
    connect_timeout: float = 10.0
    read_timeout: float = 30.0
    write_timeout: float = 10.0
    pool_timeout: float = 5.0
    max_connections: int = 100
    max_keepalive_connections: int = 20
    keepalive_expiry: float = 30.0
    retries: int = 3
    http2_enabled: bool = False
    verify_ssl: bool = False


@dataclass
class BatchConfig:
    """Batch processing configuration"""
    batch_size: int = 1000
    max_workers: int = 5
    chunk_size: int = 5


@dataclass
class ApplicationConfig:
    """Main application configuration"""
    environment: Environment = Environment.DEVELOPMENT
    database: DatabaseConfig = field(default_factory=lambda: DatabaseConfig(
        host="", port=5432, database="", username="", password="", schema=""
    ))
    events: List[EventConfig] = field(default_factory=list)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    http: HTTPConfig = field(default_factory=HTTPConfig)
    batch: BatchConfig = field(default_factory=BatchConfig)
    
    def validate(self) -> None:
        """Validate configuration"""
        errors = []
        
        # Validate database config
        if not self.database.host:
            errors.append("Database host is required")
        if not self.database.database:
            errors.append("Database name is required")
        if not self.database.username:
            errors.append("Database username is required")
        if not self.database.password:
            errors.append("Database password is required")
        
        # Validate events
        if not self.events:
            errors.append("At least one event configuration is required")
        
        for i, event in enumerate(self.events):
            if not event.token:
                errors.append(f"Event {i}: token is required")
            if not event.event_id:
                errors.append(f"Event {i}: event_id is required")
            if not event.schema:
                errors.append(f"Event {i}: schema is required")
            if not event.region:
                errors.append(f"Event {i}: region is required")
            if not event.base_url:
                errors.append(f"Event {i}: base_url is required")
        
        if errors:
            raise ValueError(f"Configuration validation failed: {'; '.join(errors)}")


class ConfigManager:
    """Manages application configuration"""
    
    def __init__(self, env_file: Optional[str] = None):
        self._config: Optional[ApplicationConfig] = None
        self._load_environment(env_file)
    
    def _load_environment(self, env_file: Optional[str] = None) -> None:
        """Load environment variables"""
        if env_file and os.path.exists(env_file):
            load_dotenv(env_file)
        else:
            load_dotenv()
    
    def _get_database_config(self) -> DatabaseConfig:
        """Get database configuration from environment"""
        return DatabaseConfig(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=int(os.getenv('POSTGRES_PORT', '5432')),
            database=os.getenv('POSTGRES_DB', ''),
            username=os.getenv('POSTGRES_USER', ''),
            password=os.getenv('POSTGRES_PASSWORD', ''),
            schema=os.getenv('POSTGRES_SCHEMA', 'public')
        )
    
    def _get_event_configs(self) -> List[EventConfig]:
        """Get event configurations from environment"""
        events = []
        
        # Look for EVENT_CONFIGS__{region}__{param} pattern
        configs = {}
        for key, value in os.environ.items():
            if key.startswith("EVENT_CONFIGS__"):
                parts = key.split("__")
                if len(parts) >= 3:
                    region = parts[1]
                    param = parts[2]
                    
                    if region not in configs:
                        configs[region] = {}
                    
                    if param in ["token", "event_id", "schema_name", "base_url"]:
                        configs[region][param] = value
                    configs[region]["region"] = region
        
        # Convert to EventConfig objects
        for region, config in configs.items():
            if all(k in config for k in ["token", "event_id", "schema_name", "region"]):
                events.append(EventConfig(
                    token=config["token"],
                    event_id=config["event_id"],
                    schema=config["schema_name"],
                    region=config["region"],
                    base_url=config.get("base_url", os.getenv('EVENT_API_BASE_URL', ''))
                ))
        
        return events
    
    def _get_logging_config(self) -> LoggingConfig:
        """Get logging configuration from environment"""
        return LoggingConfig(
            level=os.getenv('LOG_LEVEL', 'INFO'),
            enable_file_logging=os.getenv('ENABLE_FILE_LOGGING', 'true').lower() in ('true', '1'),
            log_directory=os.getenv('LOG_DIRECTORY', 'logs'),
            max_file_size=int(os.getenv('LOG_MAX_FILE_SIZE', str(10 * 1024 * 1024))),
            backup_count=int(os.getenv('LOG_BACKUP_COUNT', '5'))
        )
    
    def _get_http_config(self) -> HTTPConfig:
        """Get HTTP configuration from environment"""
        return HTTPConfig(
            timeout=float(os.getenv('HTTP_TIMEOUT', '30.0')),
            connect_timeout=float(os.getenv('HTTP_CONNECT_TIMEOUT', '10.0')),
            read_timeout=float(os.getenv('HTTP_READ_TIMEOUT', '30.0')),
            write_timeout=float(os.getenv('HTTP_WRITE_TIMEOUT', '10.0')),
            pool_timeout=float(os.getenv('HTTP_POOL_TIMEOUT', '5.0')),
            max_connections=int(os.getenv('HTTP_MAX_CONNECTIONS', '100')),
            max_keepalive_connections=int(os.getenv('HTTP_MAX_KEEPALIVE_CONNECTIONS', '20')),
            keepalive_expiry=float(os.getenv('HTTP_KEEPALIVE_EXPIRY', '30.0')),
            retries=int(os.getenv('HTTP_RETRIES', '3')),
            http2_enabled=os.getenv('HTTP_HTTP2_ENABLED', 'false').lower() in ('true', '1'),
            verify_ssl=os.getenv('HTTP_VERIFY_SSL', 'false').lower() in ('true', '1')
        )
    
    def _get_batch_config(self) -> BatchConfig:
        """Get batch processing configuration from environment"""
        return BatchConfig(
            batch_size=int(os.getenv('BATCH_SIZE', '1000')),
            max_workers=int(os.getenv('BATCH_MAX_WORKERS', '5')),
            chunk_size=int(os.getenv('BATCH_CHUNK_SIZE', '5'))
        )
    
    def get_config(self) -> ApplicationConfig:
        """Get application configuration"""
        if self._config is None:
            self._config = ApplicationConfig(
                environment=Environment(os.getenv('ENVIRONMENT', 'development')),
                database=self._get_database_config(),
                events=self._get_event_configs(),
                logging=self._get_logging_config(),
                http=self._get_http_config(),
                batch=self._get_batch_config()
            )
            
            # Validate configuration
            self._config.validate()
            logger.info("Configuration loaded and validated successfully")
        
        return self._config
    
    def reload_config(self) -> ApplicationConfig:
        """Reload configuration from environment"""
        self._config = None
        return self.get_config()


# Global config manager instance
config_manager = ConfigManager()


def get_config() -> ApplicationConfig:
    """Get the global application configuration"""
    return config_manager.get_config()
