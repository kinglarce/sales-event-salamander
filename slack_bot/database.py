import os
import logging
from typing import Optional, Any, List, Dict
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Base database manager with connection handling"""
    
    def __init__(self, schema: str):
        self.schema = schema
        db_url = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        self.engine = create_engine(db_url)
        self.SessionFactory = sessionmaker(bind=self.engine)

    @contextmanager
    def get_session(self) -> Session:
        """Get a database session with automatic closing"""
        session = self.SessionFactory()
        try:
            # Set schema for this session
            session.execute(text(f"SET search_path TO {self.schema}"))
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def execute_sql_file(self, filename: str, params: Optional[Dict[str, Any]] = None) -> List[Any]:
        """Execute SQL from a template file"""
        try:
            # Read SQL template
            with open(f'sql/{filename}.sql', 'r') as f:
                sql_template = f.read()

            # Replace schema placeholder
            sql_query = sql_template.replace('{SCHEMA}', self.schema)

            # Execute query
            with self.get_session() as session:
                result = session.execute(text(sql_query), params or {})
                return result.fetchall()
        except Exception as e:
            logger.error(f"Error executing SQL file {filename}: {e}")
            return [] 