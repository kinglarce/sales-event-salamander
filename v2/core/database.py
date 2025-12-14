"""
Advanced Database Management
Provides connection pooling, transaction management, and database operations.
"""

import logging
import os
from typing import Optional, Dict, Any, List, Union, Callable
from contextlib import contextmanager, asynccontextmanager
from sqlalchemy import create_engine, text, inspect, event
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import time
import threading
from functools import wraps

from .config import DatabaseConfig
from .logging import PerformanceLogger, get_logger

logger = get_logger(__name__)


class DatabaseError(Exception):
    """Base database error"""
    pass


class ConnectionError(DatabaseError):
    """Database connection error"""
    pass


class TransactionError(DatabaseError):
    """Database transaction error"""
    pass


class DatabaseManager:
    """Advanced database manager with connection pooling and monitoring"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None
        self._performance_logger = PerformanceLogger(logger)
        self._setup_engine()
    
    def _setup_engine(self) -> None:
        """Setup database engine with connection pooling"""
        try:
            # Create engine with connection pooling
            self.engine = create_engine(
                self.config.connection_url,
                poolclass=QueuePool,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600,  # Recycle connections every hour
                echo=False,  # Set to True for SQL debugging
                connect_args={
                    "options": f"-c search_path={self.config.schema}"
                }
            )
            
            # Create session factory
            self._session_factory = sessionmaker(
                bind=self.engine,
                expire_on_commit=False
            )
            
            # Add event listeners for monitoring
            self._add_event_listeners()
            
            logger.info(f"Database engine created for schema: {self.config.schema}")
            
        except Exception as e:
            logger.error(f"Failed to create database engine: {e}")
            raise ConnectionError(f"Failed to create database engine: {e}")
    
    def _add_event_listeners(self) -> None:
        """Add event listeners for monitoring"""
        
        @event.listens_for(self.engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            """Set connection parameters"""
            with dbapi_connection.cursor() as cursor:
                cursor.execute(f"SET search_path TO {self.config.schema}")
        
        @event.listens_for(self.engine, "before_cursor_execute")
        def receive_before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            """Log SQL execution start"""
            context._query_start_time = time.time()
        
        @event.listens_for(self.engine, "after_cursor_execute")
        def receive_after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            """Log SQL execution end"""
            total = time.time() - context._query_start_time
            if total > 1.0:  # Log slow queries
                logger.warning(f"Slow query ({total:.2f}s): {statement[:100]}...")
    
    def get_session(self) -> Session:
        """Get a new database session"""
        if not self._session_factory:
            raise ConnectionError("Database not initialized")
        
        session = self._session_factory()
        try:
            # Set search path for this session
            session.execute(text(f"SET search_path TO {self.config.schema}"))
            return session
        except Exception as e:
            session.close()
            raise ConnectionError(f"Failed to create session: {e}")
    
    @contextmanager
    def transaction(self, auto_commit: bool = True):
        """Context manager for database transactions"""
        session = self.get_session()
        try:
            yield session
            if auto_commit:
                session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Transaction failed: {e}")
            raise TransactionError(f"Transaction failed: {e}")
        finally:
            session.close()
    
    def execute_sql(self, sql: str, parameters: Optional[Dict] = None) -> Any:
        """Execute SQL with proper error handling"""
        with self.transaction() as session:
            try:
                result = session.execute(text(sql), parameters or {})
                return result.fetchall()
            except SQLAlchemyError as e:
                logger.error(f"SQL execution failed: {e}")
                raise DatabaseError(f"SQL execution failed: {e}")
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        try:
            inspector = inspect(self.engine)
            return table_name in inspector.get_table_names(schema=self.config.schema)
        except Exception as e:
            logger.error(f"Failed to check table existence: {e}")
            return False
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get table information"""
        try:
            inspector = inspect(self.engine)
            columns = inspector.get_columns(table_name, schema=self.config.schema)
            return {
                "name": table_name,
                "columns": [{"name": col["name"], "type": str(col["type"])} for col in columns]
            }
        except Exception as e:
            logger.error(f"Failed to get table info: {e}")
            return {}
    
    def health_check(self) -> bool:
        """Check database health"""
        try:
            with self.transaction() as session:
                session.execute(text("SELECT 1"))
                return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False
    
    def close(self) -> None:
        """Close database connections"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connections closed")
    
    def setup_schema(self, schema: str = None) -> None:
        """Set up schema and tables"""
        if schema is None:
            schema = self.config.schema or 'public'
            
        with self.engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            conn.execute(text(f"SET search_path TO {schema}"))
            
            # Drop existing tables
            if os.getenv('ENABLE_GROWTH_ANALYSIS', 'false').lower() != 'true':
                conn.execute(text(f"DROP TABLE IF EXISTS {schema}.summary_report CASCADE"))
            
            # Drop tables with both old and new names to ensure clean setup
            conn.execute(text(f"""
                -- Drop main tables
                DROP TABLE IF EXISTS {schema}.ticket_summary CASCADE;
                DROP TABLE IF EXISTS {schema}.tickets CASCADE;
                DROP TABLE IF EXISTS {schema}.events CASCADE;
                
                -- Drop other tables
                DROP TABLE IF EXISTS {schema}.ticket_age_groups CASCADE;
                DROP TABLE IF EXISTS {schema}.ticket_capacity_configs CASCADE;
                DROP TABLE IF EXISTS {schema}.event_capacity_configs CASCADE;
                DROP TABLE IF EXISTS {schema}.country_configs CASCADE;
            """))
            
            conn.commit()
            
        # Create tables using SQLAlchemy models
        from models.database import Base
        Base.metadata.create_all(self.engine)
            
        logger.info(f"Schema {schema} setup completed")


class TransactionManager:
    """Enhanced transaction manager with retry logic"""
    
    def __init__(self, db_manager: DatabaseManager, max_retries: int = 3):
        self.db_manager = db_manager
        self.max_retries = max_retries
        self.session: Optional[Session] = None
    
    def __enter__(self) -> Session:
        """Enter transaction context"""
        self.session = self.db_manager.get_session()
        return self.session
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit transaction context"""
        if self.session:
            try:
                if exc_type is None:
                    self.session.commit()
                else:
                    self.session.rollback()
            except Exception as e:
                logger.error(f"Transaction cleanup failed: {e}")
            finally:
                self.session.close()
                self.session = None


def retry_on_failure(max_retries: int = 3, delay: float = 1.0):
    """Decorator for retrying database operations"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (SQLAlchemyError, IntegrityError) as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        logger.warning(f"Database operation failed (attempt {attempt + 1}/{max_retries}): {e}")
                        time.sleep(delay * (2 ** attempt))  # Exponential backoff
                    else:
                        logger.error(f"Database operation failed after {max_retries} attempts: {e}")
                        raise DatabaseError(f"Operation failed after {max_retries} attempts: {e}")
            
            raise last_exception
        
        return wrapper
    return decorator


class DatabaseMetrics:
    """Database performance metrics"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self._metrics = {
            "connections_created": 0,
            "connections_closed": 0,
            "transactions_committed": 0,
            "transactions_rolled_back": 0,
            "queries_executed": 0,
            "slow_queries": 0
        }
        self._lock = threading.Lock()
    
    def increment(self, metric: str, value: int = 1) -> None:
        """Increment a metric"""
        with self._lock:
            if metric in self._metrics:
                self._metrics[metric] += value
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics"""
        with self._lock:
            return self._metrics.copy()
    
    def reset_metrics(self) -> None:
        """Reset all metrics"""
        with self._lock:
            for key in self._metrics:
                self._metrics[key] = 0
    
def create_database_manager(config: DatabaseConfig) -> DatabaseManager:
    """Create database manager instance"""
    return DatabaseManager(config)
