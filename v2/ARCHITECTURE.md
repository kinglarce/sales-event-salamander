# Improved Architecture Documentation

## Overview

This document describes the refactored architecture following senior software engineering best practices. The new architecture provides better maintainability, performance, error handling, and monitoring.

## Key Improvements

### 🏗️ **Architecture Patterns**

1. **Dependency Injection**: All dependencies are injected through configuration
2. **Factory Pattern**: HTTP clients and database connections are created through factories
3. **Strategy Pattern**: Retry strategies and error handling are configurable
4. **Observer Pattern**: Comprehensive logging and monitoring
5. **Context Managers**: Proper resource management with automatic cleanup

### 🔧 **Core Components**

#### 1. Configuration Management (`core/config.py`)
- **Centralized Configuration**: All settings in one place with validation
- **Environment Variables**: Automatic loading from `.env` files
- **Type Safety**: Dataclasses with proper typing
- **Validation**: Configuration validation on startup

```python
from core import get_config

config = get_config()
# Access: config.database.host, config.events[0].token, etc.
```

#### 2. HTTP Client Management (`core/http_client.py`)
- **Connection Pooling**: Efficient connection reuse
- **Retry Logic**: Exponential backoff with configurable retries
- **Error Handling**: Comprehensive error classification
- **Async Support**: Full async/await support

```python
async with VivenuHTTPClient(token, base_url) as client:
    events = await client.get_events()
    tickets = await client.get_tickets(skip=0, limit=1000)
```

#### 3. Database Management (`core/database.py`)
- **Connection Pooling**: QueuePool with pre-ping
- **Transaction Management**: Context managers for transactions
- **Retry Logic**: Automatic retry on transient failures
- **Health Checks**: Database connectivity monitoring

```python
db_manager = DatabaseManager(config.database)

with TransactionManager(db_manager) as session:
    # Database operations with automatic commit/rollback
    pass
```

#### 4. Batch Processing (`core/batch_processor.py`)
- **Parallel Processing**: Configurable worker pools
- **Progress Tracking**: Real-time progress monitoring
- **Error Recovery**: Failed batch retry mechanisms
- **Performance Metrics**: Throughput and success rate tracking

```python
batch_processor = BatchProcessor(config.batch)
stats = await batch_processor.process_batches_async(items, processor_func)
```

#### 5. Advanced Logging (`core/logging.py`)
- **Structured Logging**: JSON-formatted logs for better parsing
- **Performance Metrics**: Automatic timing and metrics collection
- **Context Filtering**: Add context to all log messages
- **Log Rotation**: Automatic log rotation with size limits

```python
logger = get_logger(__name__)
performance_logger = PerformanceLogger(logger)
performance_logger.start_timer("operation")
# ... do work ...
performance_logger.end_timer("operation", items_processed=1000)
```

### 🚀 **Improved Files**

#### 1. `ingest_events_tickets_v2.py`
- **Class-based Architecture**: `EventsTicketsIngester` class
- **Error Handling**: Custom exceptions with proper error classification
- **Validation**: Comprehensive data validation before processing
- **Monitoring**: Performance metrics and progress tracking

#### 2. `ingest_coupons_v2.py`
- **Separation of Concerns**: Dedicated `CouponProcessor` and `CouponDataLoader`
- **Retry Logic**: Automatic retry on transient failures
- **Data Validation**: Input validation and sanitization
- **Summary Updates**: Efficient summary statistics calculation

#### 3. `scripts/run_ingest_v2.py`
- **Orchestration**: `IngestOrchestrator` manages the entire process
- **Graceful Shutdown**: Signal handling for clean shutdown
- **Process Monitoring**: Track success/failure of each step
- **Comprehensive Reporting**: Detailed execution summaries

### 📊 **Performance Improvements**

1. **Connection Pooling**: Reuse HTTP and database connections
2. **Async Processing**: Non-blocking I/O operations
3. **Batch Processing**: Efficient batch operations with parallel processing
4. **Memory Management**: Proper resource cleanup and garbage collection
5. **Caching**: Intelligent caching of frequently accessed data

### 🛡️ **Error Handling & Resilience**

1. **Custom Exceptions**: Specific exception types for different error scenarios
2. **Retry Strategies**: Exponential backoff with jitter
3. **Circuit Breakers**: Prevent cascading failures
4. **Graceful Degradation**: Continue processing when non-critical components fail
5. **Health Checks**: Monitor system health and connectivity

### 📈 **Monitoring & Observability**

1. **Structured Logging**: JSON logs for easy parsing and analysis
2. **Performance Metrics**: Automatic timing and throughput measurement
3. **Health Checks**: Database and API connectivity monitoring
4. **Progress Tracking**: Real-time progress reporting
5. **Error Classification**: Categorized error reporting

### 🔧 **Configuration**

#### Environment Variables
```bash
# Database Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=your_database
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password

# HTTP Configuration
HTTP_TIMEOUT=30.0
HTTP_RETRIES=3
HTTP_MAX_CONNECTIONS=100

# Batch Processing
BATCH_SIZE=1000
BATCH_MAX_WORKERS=5

# Logging
LOG_LEVEL=INFO
ENABLE_FILE_LOGGING=true
LOG_DIRECTORY=logs
```

#### Event Configuration
```bash
# Event-specific configuration
EVENT_CONFIGS__auckland__token=your_token
EVENT_CONFIGS__auckland__event_id=your_event_id
EVENT_CONFIGS__auckland__schema_name=auckland
EVENT_CONFIGS__auckland__base_url=https://api.vivenu.com
```

### 🚀 **Usage Examples**

#### Basic Usage
```python
from core import get_config, setup_logging
from ingest_events_tickets_v2 import EventsTicketsIngester

# Setup
config = get_config()
setup_logging(config.logging)

# Run ingestion
ingester = EventsTicketsIngester(config)
result = await ingester.ingest_data(token, event_id, schema, region)
```

#### Advanced Usage with Monitoring
```python
from core import get_logger, PerformanceLogger

logger = get_logger(__name__)
perf_logger = PerformanceLogger(logger)

# Monitor performance
perf_logger.start_timer("custom_operation")
# ... do work ...
perf_logger.end_timer("custom_operation", items_processed=1000)
```

### 🔄 **Migration Guide**

#### From Old to New Architecture

1. **Replace Direct Imports**:
   ```python
   # Old
   from ingest_events_tickets import ingest_data
   
   # New
   from ingest_events_tickets_v2 import EventsTicketsIngester
   ```

2. **Update Configuration**:
   ```python
   # Old
   load_dotenv()
   token = os.getenv('TOKEN')
   
   # New
   from core import get_config
   config = get_config()
   token = config.events[0].token
   ```

3. **Use New Orchestrator**:
   ```python
   # Old
   python scripts/run_ingest.py
   
   # New
   python scripts/run_ingest_v2.py --debug
   ```

### 🧪 **Testing**

The new architecture supports comprehensive testing:

```python
# Unit tests
pytest tests/unit/

# Integration tests
pytest tests/integration/

# Performance tests
pytest tests/performance/
```

### 📚 **Best Practices Implemented**

1. **SOLID Principles**: Single responsibility, open/closed, Liskov substitution, interface segregation, dependency inversion
2. **DRY (Don't Repeat Yourself)**: Shared utilities and common patterns
3. **KISS (Keep It Simple, Stupid)**: Clear, readable code with minimal complexity
4. **YAGNI (You Aren't Gonna Need It)**: Only implement what's actually needed
5. **Separation of Concerns**: Clear boundaries between different responsibilities
6. **Error Handling**: Comprehensive error handling with proper exception hierarchy
7. **Resource Management**: Proper cleanup and resource management
8. **Monitoring**: Comprehensive logging and performance monitoring
9. **Configuration**: Centralized, validated configuration management
10. **Documentation**: Clear, comprehensive documentation

This architecture provides a solid foundation for maintainable, scalable, and reliable data ingestion processes.
