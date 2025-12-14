"""
Core Module
Provides centralized configuration, database management, HTTP clients, and batch processing.
"""

import sys
import os
from pathlib import Path

# Add project root to Python path for shared components
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from .config import (
    ApplicationConfig,
    DatabaseConfig,
    EventConfig,
    LoggingConfig,
    HTTPConfig,
    BatchConfig,
    ConfigManager,
    get_config
)

from .database import (
    DatabaseManager,
    TransactionManager,
    DatabaseError,
    ConnectionError,
    TransactionError,
    retry_on_failure,
    create_database_manager
)

from .http_client import (
    HTTPConfig as HTTPClientConfig,
    HTTPClientManager,
    VivenuHTTPClient,
    HTTPClientError,
    RetryExhaustedError
)

from .batch_processor import (
    BatchProcessor,
    BatchResult,
    ProcessingStats,
    BatchStatus,
    ProgressTracker
)

from .logging import (
    LoggerManager,
    PerformanceLogger,
    APILogger,
    setup_logging,
    get_logger
)

__all__ = [
    # Configuration
    'ApplicationConfig',
    'DatabaseConfig', 
    'EventConfig',
    'LoggingConfig',
    'HTTPConfig',
    'BatchConfig',
    'ConfigManager',
    'get_config',
    
    # Database
    'DatabaseManager',
    'TransactionManager',
    'DatabaseError',
    'ConnectionError',
    'TransactionError',
    'retry_on_failure',
    'create_database_manager',
    
    # HTTP Client
    'HTTPClientConfig',
    'HTTPClientManager',
    'VivenuHTTPClient',
    'HTTPClientError',
    'RetryExhaustedError',
    
    # Batch Processing
    'BatchProcessor',
    'BatchResult',
    'ProcessingStats',
    'BatchStatus',
    'ProgressTracker',
    
    # Logging
    'LoggerManager',
    'PerformanceLogger',
    'APILogger',
    'setup_logging',
    'get_logger'
]
