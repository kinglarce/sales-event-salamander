import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text

def generate_summary_excel(schema: str, output_path: str = "ticket_summary.xlsx"):
    """Generate Excel report from ticket summary table for specific schema"""
    
    # Connect to database
    engine = create_engine("postgresql://postgres:postgres@postgres:5432/vivenu_db")
    
    # Query ticket summary
    query = f"""
        SELECT 
            ts.event_id,
            e.name as event_name,
            ts.ticket_name,
            ts.total_count,
            ts.updated_at
        FROM {schema}.ticket_type_summary ts
        JOIN {schema}.events e ON ts.event_id = e.id
        ORDER BY e.name, ts.ticket_name
    """
    
    # Create DataFrame and save to Excel
    df = pd.read_sql(text(query), engine)
    df.to_excel(output_path, index=False)
    print(f"Excel report generated for schema {schema}: {output_path}")

if __name__ == "__main__":
    generate_summary_excel("default") 