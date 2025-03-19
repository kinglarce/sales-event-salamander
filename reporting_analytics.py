import os
import logging
import pandas as pd
import numpy as np
import json
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv
from typing import Dict, List, Optional, Any, Tuple
import pytz
from io import BytesIO
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Always log to console
    ]
)

# Check if file logging is enabled
if os.getenv('ENABLE_FILE_LOGGING', 'true').strip().lower() in ('true', '1'):
    log_filename = f'logs/age_group_analytics_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(file_handler)

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Handles database connections and queries"""
    
    def __init__(self, schema: str):
        self.schema = schema
        # Use environment variables for database connection
        db_url = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        self.engine = create_engine(db_url)
        
    def execute_query(self, query: str, params: Dict = None) -> List:
        """Execute a SQL query with parameters"""
        try:
            with self.engine.connect() as conn:
                # Ensure query is a string and wrap it in text()
                query_text = text(query) if isinstance(query, str) else query
                result = conn.execute(query_text, params or {})
                return result.fetchall()
        except Exception as e:
            logger.error(f"Database query error: {e}")
            return []
    
    def close(self):
        """Close the database connection"""
        self.engine.dispose()

class AgeGroupDataProvider:
    """Provides age group data from the database"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.schema = db_manager.schema
        
    def get_age_group_data(self) -> pd.DataFrame:
        """Get age group data from the database"""
        try:
            query = f"""
                SELECT 
                    ticket_group,
                    age_range,
                    count
                FROM {self.schema}.ticket_age_groups
                ORDER BY 
                    CASE 
                        -- Singles
                        WHEN ticket_group = 'HYROX MEN' THEN 1
                        WHEN ticket_group = 'HYROX WOMEN' THEN 2
                        WHEN ticket_group = 'HYROX PRO MEN' THEN 3
                        WHEN ticket_group = 'HYROX PRO WOMEN' THEN 4
                        WHEN ticket_group = 'HYROX ADAPTIVE MEN' THEN 5
                        WHEN ticket_group = 'HYROX ADAPTIVE WOMEN' THEN 6
                        -- Doubles
                        WHEN ticket_group = 'HYROX DOUBLES MEN' THEN 10
                        WHEN ticket_group = 'HYROX DOUBLES WOMEN' THEN 11
                        WHEN ticket_group = 'HYROX DOUBLES MIXED' THEN 12
                        WHEN ticket_group = 'HYROX PRO DOUBLES MEN' THEN 13
                        WHEN ticket_group = 'HYROX PRO DOUBLES WOMEN' THEN 14
                        -- Relays
                        WHEN ticket_group = 'HYROX MENS RELAY' THEN 20
                        WHEN ticket_group = 'HYROX WOMENS RELAY' THEN 21
                        WHEN ticket_group = 'HYROX MIXED RELAY' THEN 22
                        WHEN ticket_group = 'HYROX MENS CORPORATE RELAY' THEN 23
                        WHEN ticket_group = 'HYROX WOMENS CORPORATE RELAY' THEN 24
                        WHEN ticket_group = 'HYROX MIXED CORPORATE RELAY' THEN 25
                        ELSE 99
                    END,
                    CASE 
                        WHEN age_range = 'U24' THEN 1
                        WHEN age_range = '25-29' THEN 2
                        WHEN age_range = '30-34' THEN 3
                        WHEN age_range = '35-39' THEN 4
                        WHEN age_range = '40-44' THEN 5
                        WHEN age_range = '45-49' THEN 6
                        WHEN age_range = '50-54' THEN 7
                        WHEN age_range = '55-59' THEN 8
                        WHEN age_range = '60-64' THEN 9
                        WHEN age_range = '65-69' THEN 10
                        WHEN age_range = '70+' THEN 11
                        WHEN age_range = 'Total' THEN 12
                        ELSE 99
                    END
            """
            
            results = self.db.execute_query(query)
            
            # Convert to DataFrame
            df = pd.DataFrame(results, columns=['ticket_group', 'age_range', 'count'])
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting age group data: {e}")
            return pd.DataFrame()

    def get_event_info(self) -> Dict:
        """Get event information from database"""
        try:
            query = f"""
                SELECT name, start_date, end_date
                FROM {self.schema}.events
                LIMIT 1
            """
            result = self.db.execute_query(query)
            if result:
                return {
                    'name': result[0][0],
                    'start_date': result[0][1],
                    'end_date': result[0][2]
                }
            return {}
        except Exception as e:
            logger.error(f"Error getting event info: {e}")
            return {}

class SlackReporter:
    """Handles Slack reporting"""
    
    @staticmethod
    def load_icon_mapping():
        try:
            with open("icons.json", "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"default": "🎟️"}
    
    def __init__(self, schema: str, region: str):
        self.schema = schema
        self.region = region
        self.slack_token = os.getenv('SLACK_API_TOKEN')
        self.slack_channel = os.getenv(f'EVENT_CONFIGS__{region}__SLACK_CHANNEL', '#event-analytics')
        
        if self.slack_token:
            self.slack_client = WebClient(token=self.slack_token)
        else:
            self.slack_client = None
            
    def format_age_group_table(self, df: pd.DataFrame) -> List[Dict]:
        """Format age group data as a Slack message with tables"""
        if df.empty:
            return [{"type": "section", "text": {"type": "mrkdwn", "text": "No age group data available."}}]
        
        icon_mapping = self.load_icon_mapping()
        # Get the icon based on the schema (which is the region)
        icon = icon_mapping.get(self.region, icon_mapping["default"])
        
        blocks = []
        
        blocks.append({
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{icon} {self.schema.upper()} Age Group Distribution"
            }
        })
        
        # Group by ticket_group
        groups = df['ticket_group'].unique()
        
        # Create tables for each group (or batch of groups)
        for i in range(0, len(groups), 2):  # Process 2 groups at a time
            batch_groups = groups[i:i+2]
            table_text = "```\n"
            
            # Get data for each group in this batch
            group_dfs = []
            max_rows = 0
            for group in batch_groups:
                group_df = df[df['ticket_group'] == group].copy()
                group_dfs.append(group_df)
                max_rows = max(max_rows, len(group_df))
            
            # Create headers with fixed width
            for group in batch_groups:
                table_text += f"{group:<35} | "
            table_text = table_text.rstrip(" | ") + "\n"
            
            # Add separator
            for _ in batch_groups:
                table_text += f"{'-'*35} | "
            table_text = table_text.rstrip(" | ") + "\n"
            
            # Create rows
            age_ranges = ['U24', '25-29', '30-34', '35-39', '40-44', '45-49', 
                         '50-54', '55-59', '60-64', '65-69', '70+', 'Total']
            
            for age_range in age_ranges:
                line = ""
                for idx, group in enumerate(batch_groups):
                    group_df = group_dfs[idx]
                    row = group_df[group_df['age_range'] == age_range]
                    if not row.empty:
                        count = row['count'].values[0]
                        line += f"{age_range:<15} {count:>19} | "
                    else:
                        line += f"{age_range:<15} {0:>19} | "
                
                if line:  # Only add non-empty lines
                    table_text += line.rstrip(" | ") + "\n"
            
            table_text += "```"
            
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": table_text
                }
            })
            
            # Add a divider between batches
            if i + 2 < len(groups):
                blocks.append({"type": "divider"})
        
        return blocks
        
    def send_report(self, df: pd.DataFrame) -> bool:
        """Send age group report to Slack"""
        if self.slack_client is None:
            logger.warning("Slack client not initialized. Cannot send report.")
            return False
        
        try:
            # Format the data for Slack
            blocks = self.format_age_group_table(df)
            
            # Add fallback text
            event_name = os.getenv(f'EVENT_CONFIGS__{self.region}__event_name', 'Event')
            fallback_text = f"{event_name} Age Group Distribution Report"
            
            # Send message with blocks
            response = self.slack_client.chat_postMessage(
                channel=self.slack_channel,
                blocks=blocks,
                text=fallback_text
            )
            
            logger.info(f"Slack report sent successfully to {self.slack_channel}")
            return True
            
        except SlackApiError as e:
            logger.error(f"Error sending Slack report: {e.response['error']}")
            logger.error(f"Error details: {e.response}")
            return False
        except Exception as e:
            logger.error(f"Error preparing or sending report: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

class AgeGroupAnalytics:
    """Main class that orchestrates the age group analytics process"""
    
    def __init__(self, schema: str, region: str):
        self.schema = schema
        self.region = region
        
        # Initialize components
        self.db_manager = DatabaseManager(schema)
        self.data_provider = AgeGroupDataProvider(self.db_manager)
        self.reporter = SlackReporter(schema, region)
    
    def create_excel_file(self, df: pd.DataFrame) -> None:
        """Create an Excel file with age group data and save it to the excels directory"""
        if df.empty:
            logger.warning("No data available to create Excel file.")
            return
            
        # Get event info
        event_info = self.data_provider.get_event_info()
        event_name = event_info.get('name', 'Event')
        start_date = event_info.get('start_date')
        end_date = event_info.get('end_date')
        
        # Format event date
        event_date = ''
        if start_date and end_date:
            if start_date.date() == end_date.date():
                event_date = start_date.strftime("%m/%d/%Y")
            else:
                event_date = f"{start_date.strftime('%m/%d/%Y')} - {end_date.strftime('%m/%d/%Y')}"
        
        # Create the excels directory if it doesn't exist
        os.makedirs('excels', exist_ok=True)
        
        # Create Excel file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f'excels/Age_Groups_{self.region}_{timestamp}.xlsx'
        
        # Define the layout structure with titles
        layout = [
            {
                'title': 'SINGLES',
                'groups': ['HYROX MEN', 'HYROX WOMEN', 'HYROX PRO MEN', 'HYROX PRO WOMEN', 'HYROX ADAPTIVE MEN', 'HYROX ADAPTIVE WOMEN']
            },
            {
                'title': 'DOUBLES',
                'groups': ['HYROX DOUBLES MEN', 'HYROX DOUBLES WOMEN', 'HYROX DOUBLES MIXED', 'HYROX PRO DOUBLES MEN', 'HYROX PRO DOUBLES WOMEN']
            },
            {
                'title': 'RELAYS',
                'groups': ['HYROX MENS RELAY', 'HYROX WOMENS RELAY', 'HYROX MIXED RELAY', 'HYROX MENS CORPORATE RELAY', 'HYROX WOMENS CORPORATE RELAY', 'HYROX MIXED CORPORATE RELAY']
            }
        ]
        
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            workbook = writer.book
            worksheet = workbook.add_worksheet('Age Groups')
            
            # Add formats
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'border': 1,
                'align': 'center'
            })
            
            date_format = workbook.add_format({
                'bold': True,
                'align': 'left'
            })
            
            total_format = workbook.add_format({
                'bold': True
            })
            
            # Add a section title format
            section_format = workbook.add_format({
                'bold': True,
                'font_size': 14,
                'align': 'left'
            })
            
            # Write headers
            hkt_tz = pytz.timezone('Asia/Hong_Kong')
            current_time = datetime.now(hkt_tz)
            worksheet.write('A1', f'Last Data Update: {current_time.strftime("%d %B %Y %I:%M%p")} HKT', date_format)
            worksheet.write('A2', f'Event Name: {event_name}', date_format)
            worksheet.write('A3', f'Event Date: {event_date}', date_format)
            
            current_row = 5  # Start after the headers
            table_height = 14  # Header + column headers + 11 age ranges + total
            
            # Process each section in the layout
            for section in layout:
                # Write section title
                worksheet.write(current_row, 0, section['title'], section_format)
                current_row += 2  # Add space after title
                
                # Process each group in the current row
                for col_idx, group in enumerate(section['groups']):
                    col_offset = col_idx * 3  # Each table takes 2 columns + 1 spacing column
                    
                    group_data = df[df['ticket_group'] == group]
                    if not group_data.empty:
                        # Merge cells and write group header
                        worksheet.merge_range(
                            current_row, col_offset, 
                            current_row, col_offset + 1, 
                            group, header_format
                        )
                        
                        # Write column headers
                        worksheet.write(current_row + 1, col_offset, 'age_range', header_format)
                        worksheet.write(current_row + 1, col_offset + 1, 'count', header_format)
                        
                        # Write data rows
                        age_order = ['U24', '25-29', '30-34', '35-39', '40-44', '45-49', 
                                    '50-54', '55-59', '60-64', '65-69', '70+']
                        
                        for row_idx, age_range in enumerate(age_order):
                            row_data = group_data[group_data['age_range'] == age_range]
                            count = row_data['count'].iloc[0] if not row_data.empty else 0
                            worksheet.write(current_row + 2 + row_idx, col_offset, age_range)
                            worksheet.write(current_row + 2 + row_idx, col_offset + 1, count)
                        
                        # Write total
                        total_row = group_data[group_data['age_range'] == 'Total']
                        total = total_row['count'].iloc[0] if not total_row.empty else 0
                        worksheet.write(current_row + 13, col_offset, 'Total', total_format)
                        worksheet.write(current_row + 13, col_offset + 1, total, total_format)
                
                # Move to next row of tables with spacing
                current_row += table_height + 3  # Add 3 for more spacing between sections
            
            # Set column widths
            max_columns = 18  # Maximum 6 tables per row * (2 columns + 1 spacing column)
            for i in range(max_columns):
                if i % 3 == 2:  # Every third column is a spacing column
                    worksheet.set_column(i, i, 3)  # Make spacing smaller to fit all tables
                else:
                    worksheet.set_column(i, i, 12)  # Make columns slightly smaller to fit all tables
            
            logger.info(f"Excel file created: {filename}")
    
    def run_analysis(self):
        """Run the complete analysis workflow"""
        try:
            # Get age group data
            age_group_data = self.data_provider.get_age_group_data()
            logger.info(f"Age group data shape: {age_group_data.shape if not age_group_data.empty else 'Empty'}")
            
            # Create Excel file
            self.create_excel_file(age_group_data)
            
            logger.info(f"Analysis completed successfully for schema {self.schema}")
            
        except Exception as e:
            logger.error(f"Error running analysis: {e}", exc_info=True)
        finally:
            self.db_manager.close()

def main():
    parser = argparse.ArgumentParser(description='Age Group Analytics')
    parser.add_argument('--slack', action='store_true', help='Send report to Slack')
    parser.add_argument('--excel', action='store_true', help='Generate Excel report')
    args = parser.parse_args()
    
    load_dotenv()
    
    if not (args.slack or args.excel):
        logger.error("Please specify at least one output format: --slack or --excel")
        return
    
    # Get event configurations
    configs = []
    for key, value in os.environ.items():
        if key.startswith("EVENT_CONFIGS__") and key.endswith("__schema_name"):
            region = key.split("__")[1]
            schema = value
            configs.append({"schema": schema, "region": region})
    
    if not configs:
        logger.error("No valid event configurations found")
        return
    
    # Run analysis for each event
    for config in configs:
        logger.info(f"Running age group analysis for schema: {config['schema']}")
        analyzer = AgeGroupAnalytics(config['schema'], config['region'])
        
        try:
            # Get age group data
            age_group_data = analyzer.data_provider.get_age_group_data()
            
            if args.excel:
                analyzer.create_excel_file(age_group_data)
            
            if args.slack:
                success = analyzer.reporter.send_report(age_group_data)
                logger.info(f"Slack report sent: {success}")
                
        except Exception as e:
            logger.error(f"Error processing {config['schema']}: {e}")
            continue
        finally:
            analyzer.db_manager.close()

if __name__ == "__main__":
    main() 