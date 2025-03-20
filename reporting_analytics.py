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
    handlers=[logging.StreamHandler()]
)

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
        db_url = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        self.engine = create_engine(db_url)
        
    def execute_query(self, query: str, params: Dict = None) -> List:
        try:
            with self.engine.connect() as conn:
                query_text = text(query) if isinstance(query, str) else query
                result = conn.execute(query_text, params or {})
                return result.fetchall()
        except Exception as e:
            logger.error(f"Database query error: {e}")
            return []
    
    def close(self):
        self.engine.dispose()

class DataProvider:
    """Provides data from the database"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.schema = db_manager.schema
        
    def get_age_group_data(self) -> pd.DataFrame:
        try:
            query = f"""
                SELECT 
                    ticket_group,
                    age_range,
                    count
                FROM {self.schema}.ticket_age_groups
                ORDER BY 
                    CASE 
                        WHEN ticket_group = 'HYROX MEN' THEN 1
                        WHEN ticket_group = 'HYROX WOMEN' THEN 2
                        WHEN ticket_group = 'HYROX PRO MEN' THEN 3
                        WHEN ticket_group = 'HYROX PRO WOMEN' THEN 4
                        WHEN ticket_group = 'HYROX ADAPTIVE MEN' THEN 5
                        WHEN ticket_group = 'HYROX ADAPTIVE WOMEN' THEN 6
                        WHEN ticket_group = 'HYROX DOUBLES MEN' THEN 10
                        WHEN ticket_group = 'HYROX DOUBLES WOMEN' THEN 11
                        WHEN ticket_group = 'HYROX DOUBLES MIXED' THEN 12
                        WHEN ticket_group = 'HYROX PRO DOUBLES MEN' THEN 13
                        WHEN ticket_group = 'HYROX PRO DOUBLES WOMEN' THEN 14
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
            return pd.DataFrame(results, columns=['ticket_group', 'age_range', 'count'])
        except Exception as e:
            logger.error(f"Error getting age group data: {e}")
            return pd.DataFrame()

    def get_event_info(self) -> Dict:
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

class SlackService:
    """Handles Slack communication"""
    
    def __init__(self, schema: str, region: str):
        self.schema = schema
        self.region = region
        self.slack_token = os.getenv('SLACK_API_TOKEN')
        self.channel_name = os.getenv(f'EVENT_CONFIGS__{region}__SLACK_CHANNEL', '#event-analytics')
        self.channel_id = None
        
        if self.slack_token:
            self.client = WebClient(token=self.slack_token)
            self.channel_id = self._get_channel_id()
        else:
            self.client = None
            logger.warning("Slack client not initialized: missing API token")

    def _get_channel_id(self) -> Optional[str]:
        """Get and cache the channel ID"""
        try:
            channel_name = self.channel_name.lstrip('#')
            response = self.client.conversations_list(
                types="private_channel",
                exclude_archived=True
            )
            
            if response['ok']:
                for channel in response['channels']:
                    if channel['name'] == channel_name:
                        logger.info(f"Found channel ID for {channel_name}: {channel['id']}")
                        return channel['id']
            
            logger.error(f"Channel not found: {channel_name}")
            return None
            
        except SlackApiError as e:
            logger.error(f"Error getting channel ID: {e.response['error']}")
            return None

    def send_report(self, df: pd.DataFrame) -> bool:
        """Send formatted report to Slack"""
        if not self.client or not self.channel_id:
            return False

        try:
            blocks = self._format_age_group_table(df)
            event_name = os.getenv(f'EVENT_CONFIGS__{self.region}__event_name', 'Event')
            
            response = self.client.chat_postMessage(
                channel=self.channel_id,
                blocks=blocks,
                text=f"{event_name} Age Group Distribution Report"
            )
            
            logger.info(f"Slack report sent successfully to {self.channel_name}")
            return True
            
        except SlackApiError as e:
            logger.error(f"Error sending Slack report: {e.response['error']}")
            return False

    def send_excel_report(self, file_path: str, initial_comment: str) -> bool:
        """Send Excel file to Slack"""
        if not self.client or not self.channel_id:
            return False

        try:
            with open(file_path, 'rb') as file:
                response = self.client.files_upload_v2(
                    channel=self.channel_id,
                    initial_comment=initial_comment,
                    file=file,
                    filename=os.path.basename(file_path)
                )
                logger.info(f"Excel file uploaded successfully: {response['file']['name']}")
                return True
                
        except SlackApiError as e:
            logger.error(f"Error uploading file: {e.response['error']}")
            logger.error(f"Error details: {e.response}")
            return False
        except Exception as e:
            logger.error(f"Error uploading file: {e}")
            return False

    def _format_age_group_table(self, df: pd.DataFrame) -> List[Dict]:
        """Format age group data for Slack display"""
        if df.empty:
            return [{"type": "section", "text": {"type": "mrkdwn", "text": "No age group data available."}}]
        
        icon_mapping = self._load_icon_mapping()
        icon = icon_mapping.get(self.region, icon_mapping["default"])
        
        blocks = []
        blocks.append({
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{icon} {self.schema.upper()} Age Group Distribution"
            }
        })
        
        groups = df['ticket_group'].unique()
        for i in range(0, len(groups), 2):
            batch_groups = groups[i:i+2]
            table_text = self._create_table_text(df, batch_groups)
            
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": table_text
                }
            })
            
            if i + 2 < len(groups):
                blocks.append({"type": "divider"})
        
        return blocks

    def _load_icon_mapping(self) -> Dict:
        try:
            with open("icons.json", "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"default": "🎟️"}

    def _create_table_text(self, df: pd.DataFrame, groups: List[str]) -> str:
        """Create formatted table text for Slack message"""
        table_text = "```\n"
        
        # Headers
        for group in groups:
            table_text += f"{group:<35} | "
        table_text = table_text.rstrip(" | ") + "\n"
        
        # Separator
        for _ in groups:
            table_text += f"{'-'*35} | "
        table_text = table_text.rstrip(" | ") + "\n"
        
        # Data rows
        age_ranges = ['U24', '25-29', '30-34', '35-39', '40-44', '45-49', 
                      '50-54', '55-59', '60-64', '65-69', '70+', 'Incomplete', 'Total']
        
        for age_range in age_ranges:
            line = ""
            for group in groups:
                row = df[(df['ticket_group'] == group) & (df['age_range'] == age_range)]
                count = row['count'].values[0] if not row.empty else 0
                line += f"{age_range:<15} {count:>19} | "
            table_text += line.rstrip(" | ") + "\n"
        
        table_text += "```"
        return table_text

    def _get_age_ranges_for_category(self, df: pd.DataFrame, group: str) -> List[str]:
        """Get appropriate age ranges based on ticket category"""
        if 'DOUBLES' in group:
            return ['U29', '30-39', '40-49', '50-59', '60-69', '70+', 'Incomplete', 'Total']
        elif 'RELAY' in group:
            return ['U40', '40+', 'Incomplete', 'Total']
        else:  # Singles
            return ['U24', '25-29', '30-34', '35-39', '40-44', '45-49', 
                    '50-54', '55-59', '60-64', '65-69', '70+', 'Incomplete', 'Total']

class ExcelGenerator:
    """Handles Excel report generation"""
    
    def create_report(self, df: pd.DataFrame, event_info: Dict, schema: str, region: str) -> str:
        """Create Excel report and return file path"""
        if df.empty:
            logger.warning("No data available to create Excel file.")
            return ""
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f'excels/{region.upper()}_report_{timestamp}.xlsx'
        os.makedirs('excels', exist_ok=True)
        
        try:
            with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
                self._generate_excel_content(writer, df, event_info)
            logger.info(f"Excel file created: {filename}")
            return filename
        except Exception as e:
            logger.error(f"Error creating Excel file: {e}")
            return ""

    def _generate_excel_content(self, writer: pd.ExcelWriter, df: pd.DataFrame, event_info: Dict):
        """Generate Excel content with formatting"""
        workbook = writer.book
        worksheet = workbook.add_worksheet('Age Groups')
        
        # Add formats
        title_format = workbook.add_format({
            'bold': True, 
            'font_size': 14, 
            'align': 'left'
        })
        header_format = workbook.add_format({
            'bold': True, 
            'text_wrap': True, 
            'valign': 'top', 
            'border': 1, 
            'align': 'center',
            'bg_color': '#D3D3D3'  # Light gray background
        })
        date_format = workbook.add_format({
            'bold': True, 
            'align': 'left'
        })
        total_format = workbook.add_format({
            'bold': True,
            'border': 1,
            'bg_color': '#F0F0F0'  # Lighter gray for totals
        })
        section_format = workbook.add_format({
            'bold': True, 
            'font_size': 12, 
            'align': 'left',
            'bg_color': '#E0E0E0'  # Medium gray for section headers
        })
        
        # Write event information
        hkt_tz = pytz.timezone('Asia/Hong_Kong')
        current_time = datetime.now(hkt_tz)
        event_name = event_info.get('name', 'N/A')
        start_date = event_info.get('start_date', 'N/A')
        if isinstance(start_date, datetime):
            start_date = start_date.strftime('%m/%d/%Y')
        end_date = event_info.get('end_date', 'N/A')
        if isinstance(end_date, datetime):
            end_date = end_date.strftime('%m/%d/%Y')
            
        worksheet.write('A1', f'Event: {event_name}', title_format)
        worksheet.write('A2', f'Event Commence Date: {start_date} - {end_date}', date_format)
        worksheet.write('A3', f'Last updated: {current_time.strftime("%d %B %Y %I:%M%p")} HKT', date_format)
        
        # Get unique ticket groups and age ranges
        ticket_groups = sorted(df['ticket_group'].unique())
        age_ranges = ['U24', '25-29', '30-34', '35-39', '40-44', '45-49', 
                     '50-54', '55-59', '60-64', '65-69', '70+', 'Incomplete', 'Total']
        
        current_row = 4
        max_col = 0
        
        # Define categories and their ticket groups
        categories = {
            'Individual': ['HYROX MEN', 'HYROX WOMEN', 'HYROX PRO MEN', 'HYROX PRO WOMEN',
                         'HYROX ADAPTIVE MEN', 'HYROX ADAPTIVE WOMEN'],
            'Doubles': ['HYROX DOUBLES MEN', 'HYROX DOUBLES WOMEN', 'HYROX DOUBLES MIXED',
                       'HYROX PRO DOUBLES MEN', 'HYROX PRO DOUBLES WOMEN'],
            'Relay': ['HYROX MENS RELAY', 'HYROX WOMENS RELAY', 'HYROX MIXED RELAY'],
            'Corporate Relay': ['HYROX MENS CORPORATE RELAY', 'HYROX WOMENS CORPORATE RELAY',
                              'HYROX MIXED CORPORATE RELAY']
        }
        
        def get_age_ranges_for_category(category: str) -> List[str]:
            if 'Doubles' in category:
                return ['U29', '30-39', '40-49', '50-59', '60-69', '70+', 'Incomplete', 'Total']
            elif 'Relay' in category:
                return ['U40', '40+', 'Incomplete', 'Total']
            else:  # Singles
                return ['U24', '25-29', '30-34', '35-39', '40-44', '45-49', 
                       '50-54', '55-59', '60-64', '65-69', '70+', 'Incomplete', 'Total']

        # Write data for each category
        for category, groups in categories.items():
            existing_groups = [g for g in groups if g in ticket_groups]
            if not existing_groups:
                continue
                
            # Write category header
            worksheet.merge_range(current_row, 0, current_row, len(age_ranges), 
                                category, section_format)
            current_row += 1
            
            # Get appropriate age ranges for this category
            age_ranges = get_age_ranges_for_category(category)
            
            # Write age range headers
            worksheet.write(current_row, 0, "Age Range", header_format)
            for col, age_range in enumerate(age_ranges, 1):
                worksheet.write(current_row, col, age_range, header_format)
            current_row += 1
            
            # Write data for each group
            for group in existing_groups:
                worksheet.write(current_row, 0, group, header_format)
                for col, age_range in enumerate(age_ranges, 1):
                    count = df[(df['ticket_group'] == group) & 
                             (df['age_range'] == age_range)]['count'].values
                    value = count[0] if len(count) > 0 else 0
                    format_to_use = total_format if age_range == 'Total' else None
                    worksheet.write(current_row, col, value, format_to_use)
                current_row += 1
            
            # Update max column width
            max_col = max(max_col, len(age_ranges))
            
            # Add spacing between categories
            current_row += 2
        
        # Set column widths
        worksheet.set_column(0, 0, 35)  # Ticket group column
        worksheet.set_column(1, max_col, 12)  # Age range columns
        
        # Freeze panes
        worksheet.freeze_panes(5, 1)  # Freeze after event info and headers

class Analytics:
    """Main analytics coordinator"""
    
    def __init__(self, schema: str, region: str):
        self.schema = schema
        self.region = region
        self.db_manager = DatabaseManager(schema)
        self.data_provider = DataProvider(self.db_manager)
        self.slack_service = SlackService(schema, region)
        self.excel_generator = ExcelGenerator()
    
    def process_analytics(self, send_slack: bool = False, generate_excel: bool = False) -> bool:
        """Process analytics with specified output options"""
        try:
            age_group_data = self.data_provider.get_age_group_data()
            if age_group_data.empty:
                logger.warning(f"No data available for {self.schema}")
                return False

            event_info = self.data_provider.get_event_info()
            results = []
            
            if generate_excel:
                excel_path = self.excel_generator.create_report(
                    age_group_data,
                    event_info,
                    self.schema,
                    self.region
                )
                results.append(bool(excel_path))
                
                if send_slack and excel_path:
                    success = self.slack_service.send_excel_report(
                        excel_path,
                        f"Age Group Distribution Report for {event_info.get('name', 'Event')}"
                    )
                    results.append(success)

            if send_slack:
                success = self.slack_service.send_report(age_group_data)
                results.append(success)

            return all(results)

        except Exception as e:
            logger.error(f"Error processing analytics for {self.schema}: {e}", exc_info=True)
            return False
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

    configs = []
    for key, value in os.environ.items():
        if key.startswith("EVENT_CONFIGS__") and key.endswith("__schema_name"):
            region = key.split("__")[1]
            schema = value
            configs.append({"schema": schema, "region": region})
    
    if not configs:
        logger.error("No valid event configurations found")
        return

    for config in configs:
        logger.info(f"Processing analytics for schema: {config['schema']}")
        analyzer = Analytics(config['schema'], config['region'])
        success = analyzer.process_analytics(args.slack, args.excel)
        logger.info(f"Analytics processing {'completed successfully' if success else 'failed'} for {config['schema']}")

if __name__ == "__main__":
    main() 