"""
Migration script to convert addons column from JSON to String
Run this if you need to convert existing data
"""
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from utils.db_utils import get_database_url

load_dotenv()

def migrate_addons_column(schema: str):
    """Convert addons column from JSON to String"""
    engine = create_engine(get_database_url())
    
    with engine.connect() as conn:
        # Set search path
        conn.execute(text(f"SET search_path TO {schema}"))
        
        # Check current data type
        result = conn.execute(text("""
            SELECT data_type 
            FROM information_schema.columns 
            WHERE table_name = 'tickets' 
            AND column_name = 'addons'
            AND table_schema = :schema
        """), {"schema": schema})
        
        current_type = result.fetchone()
        if current_type and current_type[0] == 'json':
            print(f"Converting addons column from JSON to String for schema {schema}")
            
            # First, extract addon names from JSON and update to string
            conn.execute(text(f"""
                UPDATE {schema}.tickets 
                SET addons = (
                    CASE 
                        WHEN addons IS NOT NULL AND jsonb_array_length(addons) > 0 
                        THEN addons->0->>'name'
                        ELSE NULL 
                    END
                )::text
                WHERE addons IS NOT NULL
            """))
            
            # Then alter the column type
            conn.execute(text(f"ALTER TABLE {schema}.tickets ALTER COLUMN addons TYPE VARCHAR USING addons::VARCHAR"))
            
            print(f"Migration completed for schema {schema}")
        else:
            print(f"Addons column is already STRING type for schema {schema}")

if __name__ == "__main__":
    # Run for all schemas you need
    schemas = ['perth', 'japan']  # Add your schemas here
    for schema in schemas:
        migrate_addons_column(schema) 