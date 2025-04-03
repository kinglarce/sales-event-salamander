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
        
    def _read_sql_file(self, filename: str) -> str:
        """Read SQL file and replace schema placeholder"""
        try:
            file_path = os.path.join('sql', filename)
            if not os.path.exists(file_path):
                logger.error(f"SQL file not found: {file_path}")
                raise FileNotFoundError(f"SQL file not found: {file_path}")
            
            with open(file_path, 'r') as f:
                sql_content = f.read().strip()
                if not sql_content:
                    logger.error(f"SQL file is empty: {file_path}")
                    raise ValueError(f"SQL file is empty: {file_path}")
                
                return sql_content.replace('{SCHEMA}', self.schema)
        except Exception as e:
            logger.error(f"Error reading SQL file {filename}: {str(e)}")
            raise
    
    def get_age_group_data(self) -> pd.DataFrame:
        try:
            query = self._read_sql_file('get_age_group_data.sql')
            results = self.db.execute_query(query)
            
            # Update to handle all 6 columns from the SQL query
            return pd.DataFrame(results, columns=[
                'ticket_group', 
                'age_range', 
                'count', 
                'ticket_event_day', 
                'display_ticket_group',
                'ticket_category'  # Add the new ticket_category column
            ])
        except Exception as e:
            logger.error(f"Error getting age group data: {e}")
            return pd.DataFrame()

    def get_event_info(self) -> Dict:
        try:
            query = self._read_sql_file('get_event_info.sql')
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

    def get_returning_athletes_data(self) -> Dict[str, int]:
        try:
            query = self._read_sql_file('get_returning_athletes.sql')
            result = self.db.execute_query(query)
            if result:
                return {
                    'returning_athletes': result[0][0] or 0,
                    'returning_to_city': result[0][1] or 0
                }
            return {'returning_athletes': 0, 'returning_to_city': 0}
        except Exception as e:
            logger.error(f"Error getting returning athletes data: {e}")
            return {'returning_athletes': 0, 'returning_to_city': 0}

    def get_region_of_residence_data(self) -> pd.DataFrame:
        try:
            query = self._read_sql_file('get_region_of_residence.sql')
            results = self.db.execute_query(query)
            return pd.DataFrame(results, columns=['region', 'count'])
        except Exception as e:
            logger.error(f"Error getting region of residence data: {e}")
            return pd.DataFrame()

    def get_gym_affiliate_data(self) -> Dict[str, Any]:
        try:
            query = self._read_sql_file('get_gym_affiliate_details.sql')
            member_details = self.db.execute_query(query)
            logger.info(f"Found {len(member_details)} gym affiliate details")
            
            # Process the results to get unique values and membership counts
            membership_counts = {}
            seen_types = set()
            ordered_unique_values = []
            member_details_list = []
            
            for row in member_details:
                membership_type = row[0]
                
                if membership_type not in seen_types:
                    ordered_unique_values.append(membership_type)
                    seen_types.add(membership_type)
                
                membership_counts[membership_type] = membership_counts.get(membership_type, 0) + row[3]
                
                member_details_list.append({
                    'membership_type': membership_type,
                    'gym': row[1],
                    'location': row[2],
                    'count': row[3]
                })
            
            return {
                'unique_values': ordered_unique_values,
                'membership_counts': membership_counts,
                'member_details': member_details_list
            }
        except Exception as e:
            logger.error(f"Error getting gym affiliate data: {e}")
            return {'unique_values': [], 'membership_counts': {}, 'member_details': []}

    def get_ticket_status_data(self) -> Dict[str, Any]:
        try:
            # 1. Get ticket status counts
            status_query = self._read_sql_file('get_ticket_status.sql')
            status_results = self.db.execute_query(status_query)
            status_counts = {row[0]: row[1] for row in status_results}
            
            # 2. Get team member counts
            team_query = self._read_sql_file('get_team_member_counts.sql')
            team_results = self.db.execute_query(team_query)
            team_counts = [
                {
                    'main_ticket_name': row[0],
                    'main_count': row[1],
                    'member_count': row[2],
                    'ticket_category': row[3],
                    'event_day': row[4],
                    'status': row[5]
                }
                for row in team_results
            ]
            
            # 3. Get gender mismatches
            gender_query = self._read_sql_file('get_gender_mismatches.sql')
            gender_results = self.db.execute_query(gender_query)
            gender_mismatches = [
                {
                    'ticket_name': row[0],
                    'gender': row[1],
                    'count': row[2],
                    'event_day': row[3],
                    'details': row[4]
                }
                for row in gender_results
            ]
            
            # 4. Get mixed pairing mismatches
            mixed_query = self._read_sql_file('get_mixed_mismatches.sql')
            mixed_results = self.db.execute_query(mixed_query)
            mixed_mismatches = [
                {
                    'ticket_name': row[0],
                    'invalid_count': row[1],
                    'details': row[2]
                }
                for row in mixed_results
            ]
            
            # 5. Get age restricted athletes
            age_query = self._read_sql_file('get_age_restricted.sql')
            age_results = self.db.execute_query(age_query)
            age_restricted = {
                'under_16': [],
                '17_to_18': []
            }
            for row in age_results:
                if row[0]:  # age_group is not None
                    age_restricted[row[0]] = row[1]
            
            # 6. Get sportograf data
            sportograf_query = self._read_sql_file('get_sportograf.sql')
            sportograf_results = self.db.execute_query(sportograf_query)
            sportograf_data = [
                {
                    'ticket_name': row[0],
                    'count': row[1]
                }
                for row in sportograf_results
            ]
            
            return {
                'status_counts': status_counts,
                'team_counts': team_counts,
                'gender_mismatches': gender_mismatches,
                'mixed_mismatches': mixed_mismatches,
                'age_restricted': age_restricted,
                'sportograf_data': sportograf_data
            }
            
        except Exception as e:
            logger.error(f"Error getting ticket status data: {e}")
            return {
                'status_counts': {},
                'team_counts': [],
                'gender_mismatches': [],
                'mixed_mismatches': [],
                'age_restricted': {'under_16': [], '17_to_18': []},
                'sportograf_data': []
            }

class SlackService:
    """Handles Slack communication"""
    
    def __init__(self, schema: str, region: str):
        self.schema = schema
        self.region = region
        self.slack_token = os.getenv('SLACK_API_TOKEN')
        # Look for both private and public channels
        self.channel_name = os.getenv(f'EVENT_CONFIGS__{region}__REPORTING_CHANNEL', '#test-hyrox-bot')
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
            
            # Try private channels first
            response = self.client.conversations_list(
                types="private_channel",
                exclude_archived=True
            )
            
            if response['ok']:
                for channel in response['channels']:
                    if channel['name'] == channel_name:
                        logger.info(f"Found private channel ID for {channel_name}: {channel['id']}")
                        return channel['id']
            
            # Try public channels if not found in private
            response = self.client.conversations_list(
                types="public_channel",
                exclude_archived=True
            )
            
            if response['ok']:
                for channel in response['channels']:
                    if channel['name'] == channel_name:
                        logger.info(f"Found public channel ID for {channel_name}: {channel['id']}")
                        return channel['id']
            
            logger.error(f"Channel not found for {self.region}: {channel_name}")
            return None
            
        except SlackApiError as e:
            logger.error(f"Error getting channel ID for {self.region}: {e.response['error']}")
            return None

    def send_report(self, df: pd.DataFrame) -> bool:
        """Send formatted report to Slack"""
        if not self.client or not self.channel_id:
            return False

        try:
            # Define the category order
            category_order = ['single', 'double', 'relay', 'corporate_relay']
            
            # Order tickets by category and then by day
            singles = sorted(df[df['ticket_category'] == 'single']['display_ticket_group'].unique(), 
                            key=lambda x: ('SATURDAY' in x, 'SUNDAY' in x, 'FRIDAY' not in x and 'SATURDAY' not in x and 'SUNDAY' not in x, x))
            doubles = sorted(df[df['ticket_category'] == 'double']['display_ticket_group'].unique(),
                            key=lambda x: ('SATURDAY' in x, 'SUNDAY' in x, 'FRIDAY' not in x and 'SATURDAY' not in x and 'SUNDAY' not in x, x))
            # Group relays and corporate relays together but keep the ordering
            relays = sorted(df[(df['ticket_category'] == 'relay') | 
                              (df['ticket_category'] == 'corporate_relay')]['display_ticket_group'].unique(),
                           key=lambda x: ('SATURDAY' in x, 'SUNDAY' in x, 'FRIDAY' not in x and 'SATURDAY' not in x and 'SUNDAY' not in x, x))

            blocks = []
            icon_mapping = self._load_icon_mapping()
            icon = icon_mapping.get(self.region, icon_mapping["default"])
            
            blocks.append({
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{icon} {self.schema.upper()} Age Group Distribution"
                }
            })

            # Process categories in the specified order
            for category_groups in [singles, doubles, relays]:
                if category_groups:
                    # Process groups in pairs
                    for i in range(0, len(category_groups), 2):
                        batch_groups = category_groups[i:i+2]
                        table_text = self._create_table_text(df, batch_groups)
                        
                        blocks.append({
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": table_text
                            }
                        })
                        
                    # Add divider between categories
                    if category_groups != relays:  # Don't add divider after last category
                        blocks.append({"type": "divider"})
            
            response = self.client.chat_postMessage(
                channel=self.channel_id,
                blocks=blocks,
                text=f"{self.schema.upper()} Age Group Distribution Report"
            )
            
            logger.info(f"Slack report sent successfully to {self.channel_name}")
            return True
            
        except SlackApiError as e:
            logger.error(f"Error sending Slack report: {e.response['error']}")
            return False

    def send_excel_report(self, file_path: str, message: str) -> bool:
        """Send Excel file to Slack"""
        if not self.client or not self.channel_id:
            logger.error(f"Cannot send Excel report for {self.region}: client or channel not initialized")
            return False

        try:
            response = self.client.files_upload_v2(
                channel=self.channel_id,
                file=file_path,
                initial_comment=message
            )
            logger.info(f"Excel report sent successfully to {self.channel_name} for {self.region}")
            return True
        except SlackApiError as e:
            logger.error(f"Error sending Excel report for {self.region}: {e.response['error']}")
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
        
        # Order display groups by day (Friday, Saturday, Sunday)
        display_groups = sorted(
            df['display_ticket_group'].unique(),
            key=lambda x: ('SATURDAY' in x, 'SUNDAY' in x, 'FRIDAY' not in x and 'SATURDAY' not in x and 'SUNDAY' not in x, x)
        )
        
        for i in range(0, len(display_groups), 2):
            batch_groups = display_groups[i:i+2]
            table_text = self._create_table_text(df, batch_groups)
            
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": table_text
                }
            })
            
            if i + 2 < len(display_groups):
                blocks.append({"type": "divider"})
        
        return blocks

    def _load_icon_mapping(self) -> Dict:
        try:
            with open("icons.json", "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"default": "🎟️"}

    def _create_table_text(self, df: pd.DataFrame, display_groups: List[str]) -> str:
        """Create formatted table text for Slack message"""
        table_text = "```\n"
        
        # Headers
        for display_group in display_groups:
            table_text += f"{display_group:<35} | "
        table_text = table_text.rstrip(" | ") + "\n"
        
        # Separator
        for _ in display_groups:
            table_text += f"{'-'*35} | "
        table_text = table_text.rstrip(" | ") + "\n"
        
        # Get appropriate age ranges based on first group's category
        first_group_data = df[df['display_ticket_group'] == display_groups[0]]
        if not first_group_data.empty:
            category = first_group_data['ticket_category'].iloc[0]
            age_ranges = self._get_age_ranges_for_category(category)
        else:
            # Fallback to default singles ranges
            age_ranges = ['U24', '25-29', '30-34', '35-39', '40-44', '45-49', 
                         '50-54', '55-59', '60-64', '65-69', '70+', 'Incomplete', 'Total']
        
        # Data rows
        for age_range in age_ranges:
            line = ""
            for display_group in display_groups:
                row = df[(df['display_ticket_group'] == display_group) & (df['age_range'] == age_range)]
                count = row['count'].values[0] if not row.empty else 0
                line += f"{age_range:<15} {count:>19} | "
            table_text += line.rstrip(" | ") + "\n"
        
        table_text += "```"
        return table_text

    def _get_age_ranges_for_category(self, category: str) -> List[str]:
        """Get appropriate age ranges based on ticket category"""
        if category == 'double':
            return ['U29', '30-39', '40-49', '50-59', '60-69', '70+', 'Incomplete', 'Total']
        elif category == 'relay' or category == 'corporate_relay':
            return ['U40', '40+', 'Incomplete', 'Total']
        else:  # Singles
            return ['U24', '25-29', '30-34', '35-39', '40-44', '45-49', 
                    '50-54', '55-59', '60-64', '65-69', '70+', 'Incomplete', 'Total']

class ExcelGenerator:
    """Handles Excel report generation"""
    
    @staticmethod
    def get_age_ranges_for_category(category: str) -> List[str]:
        # Convert category string to lowercase for consistent comparison
        category_lower = category.lower()
        
        if 'doubles' in category_lower or category_lower == 'double':
            return ['U29', '30-39', '40-49', '50-59', '60-69', '70+', 'Incomplete', 'Total']
        elif 'relay' in category_lower or category_lower == 'relay':
            return ['U40', '40+', 'Incomplete', 'Total']
        else:  # Singles or default
            return ['U24', '25-29', '30-34', '35-39', '40-44', '45-49', 
                    '50-54', '55-59', '60-64', '65-69', '70+', 'Incomplete', 'Total']
    
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
                # Add database manager to writer object
                writer.db_manager = DatabaseManager(schema)
                self._generate_excel_content(writer, df, event_info)
                self._generate_additional_stats_content(writer, event_info)
                self._generate_ticket_status_content(writer, event_info)
                # Close database connection
                writer.db_manager.close()
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
            'bg_color': '#8093B3',
            'font_color': '#FFFFFF'
        })
        date_format = workbook.add_format({
            'bold': True, 
            'align': 'left'
        })
        total_format = workbook.add_format({
            'bold': True,
            'border': 1,
            'bg_color': '#F0F0F0'
        })
        section_format = workbook.add_format({
            'bold': True, 
            'font_size': 12, 
            'border': 1, 
            'align': 'left',
            'bg_color': '#8093B3',
            'font_color': '#FFFFFF'
        })
        category_format = workbook.add_format({
            'bold': True, 
            'text_wrap': True, 
            'valign': 'top', 
            'border': 1, 
            'align': 'left',
            'bg_color': '#DFE4EC'
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
        
        current_row = 4
        max_col = 0
        
        # Define category mappings
        category_display_names = {
            'single': 'SINGLES',
            'double': 'DOUBLES',
            'relay': 'RELAY',
            'corporate_relay': 'CORPORATE RELAY'
        }
        
        # Define the order of categories
        category_order = ['single', 'double', 'relay', 'corporate_relay']
        
        # Process each category in the specific order
        for category in category_order:
            if category not in df['ticket_category'].unique():
                continue
                
            # Get display name for the category
            category_display = category_display_names.get(category, category.upper())
            
            # Filter display groups for this category and sort them by day (Friday, Saturday, Sunday)
            category_display_groups = sorted(
                df[df['ticket_category'] == category]['display_ticket_group'].unique(),
                key=lambda x: ('SATURDAY' in x, 'SUNDAY' in x, 'FRIDAY' not in x and 'SATURDAY' not in x and 'SUNDAY' not in x, x)
            )
            
            if not category_display_groups:
                continue
            
            # Get appropriate age ranges for this category
            age_ranges = self.get_age_ranges_for_category(category_display)
                
            # Write category header
            worksheet.merge_range(current_row, 0, current_row, len(age_ranges), category_display, section_format)
            current_row += 1
            
            # Write age range headers
            worksheet.write(current_row, 0, "Age Range", header_format)
            for col, age_range in enumerate(age_ranges, 1):
                worksheet.write(current_row, col, age_range, header_format)
            current_row += 1
            
            # Write data for each group
            for display_group in category_display_groups:
                worksheet.write(current_row, 0, display_group, category_format)
                for col, age_range in enumerate(age_ranges, 1):
                    count = df[(df['display_ticket_group'] == display_group) & 
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

    def _generate_additional_stats_content(self, writer: pd.ExcelWriter, event_info: Dict):
        """Generate content for the additional statistics tab"""
        workbook = writer.book
        worksheet = workbook.add_worksheet('Nationality - Gym - Returns')
        
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
            'bg_color': '#8093B3',
            'font_color': '#FFFFFF'
        })
        section_format = workbook.add_format({
            'bold': True, 
            'font_size': 12, 
            'border': 1, 
            'align': 'left',
            'bg_color': '#8093B3',
            'font_color': '#FFFFFF'
        })
        data_format = workbook.add_format({
            'align': 'left',
            'border': 1
        })
        number_format = workbook.add_format({
            'align': 'right',
            'border': 1
        })
        
        # Write event information
        event_name = event_info.get('name', 'N/A')
        worksheet.write(0, 0, f'Event: {event_name}', title_format)
        
        # Get data from DataProvider
        data_provider = DataProvider(writer.db_manager)
        
        # Left side content (starts at column 0)
        left_col = 0
        current_row = 2

        # 1. Returning Athletes Section (Left side)
        worksheet.merge_range(current_row, left_col, current_row, left_col + 1, 'Returning Athletes Statistics', section_format)
        current_row += 1
        
        returning_data = data_provider.get_returning_athletes_data()
        worksheet.write(current_row, left_col, 'Category', header_format)
        worksheet.write(current_row, left_col + 1, 'Count', header_format)
        current_row += 1
        
        worksheet.write(current_row, left_col, 'Total returning athletes', data_format)
        worksheet.write(current_row, left_col + 1, returning_data['returning_athletes'], number_format)
        current_row += 1
        
        worksheet.write(current_row, left_col, 'Total returning athletes to city', data_format)
        worksheet.write(current_row, left_col + 1, returning_data['returning_to_city'], number_format)
        current_row += 2

        # 2. Region of Residence Section (Left side)
        worksheet.merge_range(current_row, left_col, current_row, left_col + 1, 'Region of Residence Distribution', section_format)
        current_row += 1
        
        region_data = data_provider.get_region_of_residence_data()
        if not region_data.empty:
            worksheet.write(current_row, left_col, 'Region', header_format)
            worksheet.write(current_row, left_col + 1, 'Count', header_format)
            current_row += 1
            
            for _, row in region_data.iterrows():
                worksheet.write(current_row, left_col, row['region'], data_format)
                worksheet.write(current_row, left_col + 1, row['count'], number_format)
                current_row += 1

        # Right side content (starts at column 3)
        right_col = 3
        current_row = 2

        # 3. Gym Affiliate Section (Right side)
        worksheet.merge_range(current_row, right_col, current_row, right_col + 1, 'Gym Affiliate Statistics', section_format)
        current_row += 1
        
        gym_data = data_provider.get_gym_affiliate_data()
        
        # Membership Status Summary
        worksheet.write(current_row, right_col, 'Membership Status', header_format)
        worksheet.write(current_row, right_col + 1, 'Count', header_format)
        current_row += 1
        
        # Write counts for each unique membership type
        for membership_type in gym_data['unique_values']:
            count = gym_data['membership_counts'].get(membership_type, 0)
            worksheet.write(current_row, right_col, membership_type, data_format)
            worksheet.write(current_row, right_col + 1, count, number_format)
            current_row += 1
        current_row += 1

        # Process each unique membership type in separate tables
        for membership_type in gym_data['unique_values']:
            # Create section header
            title = f"Training Club Membership - {membership_type}"
            worksheet.merge_range(current_row, right_col, current_row, right_col + 3, title, section_format)
            current_row += 1

            # Headers
            worksheet.write(current_row, right_col, 'Membership Type', header_format)
            worksheet.write(current_row, right_col + 1, 'Gym', header_format)
            worksheet.write(current_row, right_col + 2, 'Location', header_format)
            worksheet.write(current_row, right_col + 3, 'Count', header_format)
            current_row += 1

            # Filter and sort member details for this membership type
            member_details = [d for d in gym_data['member_details'] 
                            if d['membership_type'] == membership_type]
            member_details.sort(key=lambda x: x['count'], reverse=True)
            
            # Always show the details, including "Not Specified" entries
            for detail in member_details:
                worksheet.write(current_row, right_col, detail['membership_type'], data_format)
                worksheet.write(current_row, right_col + 1, detail['gym'], data_format)
                worksheet.write(current_row, right_col + 2, detail['location'], data_format)
                worksheet.write(current_row, right_col + 3, detail['count'], number_format)
                current_row += 1

            current_row += 1  # Add space between tables

        # Set column widths
        # Left side
        worksheet.set_column(left_col, left_col, 35)      # Region/Category
        worksheet.set_column(left_col + 1, left_col + 1, 15)  # Count
        
        # Separator column
        worksheet.set_column(2, 2, 2)  # Small gap between sections
        
        # Right side
        worksheet.set_column(right_col, right_col, 35)    # Membership Type
        worksheet.set_column(right_col + 1, right_col + 1, 25)  # Gym
        worksheet.set_column(right_col + 2, right_col + 2, 25)  # Location
        worksheet.set_column(right_col + 3, right_col + 3, 15)  # Count

    def _generate_ticket_status_content(self, writer: pd.ExcelWriter, event_info: Dict):
        """Generate content for the ticketing status tab"""
        workbook = writer.book
        worksheet = workbook.add_worksheet('Ticketing Status')
        
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
            'bg_color': '#8093B3',
            'font_color': '#FFFFFF'
        })
        section_format = workbook.add_format({
            'bold': True, 
            'font_size': 12, 
            'border': 1, 
            'align': 'left',
            'bg_color': '#8093B3',
            'font_color': '#FFFFFF'
        })
        data_format = workbook.add_format({
            'align': 'left',
            'border': 1
        })
        number_format = workbook.add_format({
            'align': 'right',
            'border': 1
        })
        warning_format = workbook.add_format({
            'align': 'left',
            'border': 1,
            'bg_color': '#FFD7D7'  # Light red background for warnings
        })
        category_format = workbook.add_format({
            'bold': True, 
            'text_wrap': True, 
            'valign': 'top', 
            'border': 1, 
            'align': 'left',
            'bg_color': '#DFE4EC'
        })
        
        # Write event information
        event_name = event_info.get('name', 'N/A')
        worksheet.write(0, 0, f'Event: {event_name}', title_format)
        
        # Get data from DataProvider
        data_provider = DataProvider(writer.db_manager)
        ticket_status_data = data_provider.get_ticket_status_data()
        
        # Left side content
        left_col = 0
        current_row = 2
        
        # 1. Ticket Status Summary
        worksheet.merge_range(current_row, left_col, current_row, 1, 'Ticket Status Summary', section_format)
        current_row += 1
        
        worksheet.write(current_row, left_col, 'Status', header_format)
        worksheet.write(current_row, left_col + 1, 'Count', header_format)
        current_row += 1
        
        for status, count in ticket_status_data['status_counts'].items():
            worksheet.write(current_row, left_col, status, data_format)
            worksheet.write(current_row, left_col + 1, count, number_format)
            current_row += 1
        
        current_row += 2
        
        # 2. Team Member Count Verification
        worksheet.merge_range(current_row, left_col, current_row, 5, 'Team Member Count Verification', section_format)
        current_row += 1
        
        # Headers
        worksheet.write(current_row, left_col, 'Main Ticket', header_format)
        worksheet.write(current_row, left_col + 1, 'Main Count', header_format)
        worksheet.write(current_row, left_col + 2, 'Member Count', header_format)
        worksheet.write(current_row, left_col + 3, 'Category', header_format)
        worksheet.write(current_row, left_col + 4, 'Event Day', header_format)
        worksheet.write(current_row, left_col + 5, 'Status', header_format)
        current_row += 1
        
        # Group team counts by category and event day for better organization
        # Define category order
        category_order = ['single', 'double', 'relay', 'corporate_relay']
        # Define day order
        day_order = {'FRIDAY': 0, 'SATURDAY': 1, 'SUNDAY': 2, 'NONE': 3}
        
        # Sort team counts by category and then by event day
        sorted_team_counts = sorted(
            ticket_status_data['team_counts'],
            key=lambda x: (
                category_order.index(x['ticket_category']) if x['ticket_category'] in category_order else 999,
                day_order.get(x['event_day'], 999),
                x['main_ticket_name']
            )
        )
        
        # Group by category for better visual separation
        current_category = None
        current_day = None
        
        for team_count in sorted_team_counts:
            # Add a visual separator between categories
            if current_category != team_count['ticket_category']:
                if current_category is not None:
                    current_row += 0  # To add space between categories
                current_category = team_count['ticket_category']
                current_day = None
                
                # Write category header
                category_display = team_count['ticket_category'].upper()
                worksheet.merge_range(current_row, left_col, current_row, left_col + 5, category_display, section_format)
                current_row += 1
            
            # Add visual separator between days within a category
            if current_day != team_count['event_day']:
                current_day = team_count['event_day']
                
                # Write the event day as a subheader if it's not NONE
                if current_day != 'NONE':
                    worksheet.merge_range(current_row, left_col, current_row, left_col + 5, 
                                        f"{current_category.upper()} - {current_day}", category_format)
                    current_row += 1
            
            worksheet.write(current_row, left_col, team_count['main_ticket_name'], data_format)
            worksheet.write(current_row, left_col + 1, team_count['main_count'], number_format)
            worksheet.write(current_row, left_col + 2, team_count['member_count'], number_format)
            worksheet.write(current_row, left_col + 3, team_count['ticket_category'].upper(), data_format)
            worksheet.write(current_row, left_col + 4, team_count['event_day'], data_format)
            
            # Status formatting
            format_to_use = warning_format if team_count['status'] != 'OK' else data_format
            worksheet.write(current_row, left_col + 5, team_count['status'], format_to_use)
            current_row += 1
        
        # Set column widths
        worksheet.set_column(left_col, left_col, 40)      # Main ticket name
        worksheet.set_column(left_col + 1, left_col + 2, 15)  # Count columns
        worksheet.set_column(left_col + 3, left_col + 3, 12)  # Category
        worksheet.set_column(left_col + 4, left_col + 4, 12)  # Event Day
        worksheet.set_column(left_col + 5, left_col + 5, 12)  # Status
        
        current_row += 2
        
        # 3. Gender Mismatch Report
        worksheet.merge_range(current_row, left_col, current_row, 2, 'Gender Mismatch Report', section_format)
        current_row += 1
        
        # Headers for summary table
        worksheet.write(current_row, left_col, 'Ticket Type', header_format)
        worksheet.write(current_row, left_col + 1, 'Gender', header_format)
        worksheet.write(current_row, left_col + 2, 'Count', header_format)
        # worksheet.write(current_row, left_col + 3, 'Event Day', header_format)
        current_row += 1
        
        # Sort gender mismatches by event day for better organization
        sorted_gender_mismatches = sorted(
            ticket_status_data['gender_mismatches'],
            key=lambda x: (
                day_order.get(x['event_day'], 999),
                x['ticket_name']
            )
        )
        
        # Group gender mismatches by event day
        current_mismatch_day = None
        
        for mismatch in sorted_gender_mismatches:
            # Add visual separator between days
            if current_mismatch_day != mismatch['event_day']:
                if current_mismatch_day is not None:
                    current_row += 0  # To add space between days
                current_mismatch_day = mismatch['event_day']
                
                # Write the event day as a subheader if it's not NONE
                if current_mismatch_day != 'NONE':
                    worksheet.merge_range(current_row, left_col, current_row, left_col + 2, 
                                        f"GENDER MISMATCHES - {current_mismatch_day}", category_format)
                    current_row += 1
                
            worksheet.write(current_row, left_col, mismatch['ticket_name'], warning_format)
            worksheet.write(current_row, left_col + 1, mismatch['gender'], warning_format)
            worksheet.write(current_row, left_col + 2, mismatch['count'], warning_format)
            # worksheet.write(current_row, left_col + 3, mismatch['event_day'], warning_format)
            current_row += 1
        
        current_row += 2
        
        # Gender Mismatch Detailed Report
        worksheet.merge_range(current_row, left_col, current_row, 3, 'Gender Mismatch Detailed Report', section_format)
        current_row += 1
        
        worksheet.write(current_row, left_col, 'Barcode', header_format)
        worksheet.write(current_row, left_col + 1, 'Ticket Type', header_format)
        worksheet.write(current_row, left_col + 2, 'Category', header_format)
        worksheet.write(current_row, left_col + 3, 'Gender', header_format)
        # worksheet.write(current_row, left_col + 4, 'Event Day', header_format)
        current_row += 1
        
        # Reset for detailed report
        current_mismatch_day = None
        
        for mismatch in sorted_gender_mismatches:
            # Add visual separator between days in detailed report
            if current_mismatch_day != mismatch['event_day']:
                if current_mismatch_day is not None:
                    current_row += 0  # To add space between days
                current_mismatch_day = mismatch['event_day']
                
                # Write the event day as a subheader if it's not NONE
                if current_mismatch_day != 'NONE':
                    worksheet.merge_range(current_row, left_col, current_row, left_col + 3,
                                        f"GENDER MISMATCHES DETAILS - {current_mismatch_day}", category_format)
                    current_row += 1
            
            for detail in mismatch['details']:
                worksheet.write(current_row, left_col, detail['barcode'], warning_format)
                worksheet.write(current_row, left_col + 1, mismatch['ticket_name'], warning_format)
                worksheet.write(current_row, left_col + 2, detail['category_name'], warning_format)
                worksheet.write(current_row, left_col + 3, mismatch['gender'], warning_format)
                # worksheet.write(current_row, left_col + 4, mismatch['event_day'], warning_format)
                current_row += 1
        
        current_row += 2
        
        # 4. Mixed Pairing Mismatch Report
        worksheet.merge_range(current_row, left_col, current_row, left_col + 7, 'Mixed Pairing Mismatch Report', section_format)
        current_row += 1
        
        worksheet.write(current_row, left_col, 'Ticket Type', header_format)
        worksheet.write(current_row, left_col + 1, 'Transaction ID', header_format)
        worksheet.write(current_row, left_col + 2, 'Main Barcode', header_format)
        worksheet.write(current_row, left_col + 3, 'Main Gender', header_format)
        worksheet.write(current_row, left_col + 4, 'Partner Barcode', header_format)
        worksheet.write(current_row, left_col + 5, 'Partner Gender', header_format)
        worksheet.write(current_row, left_col + 6, 'Wrong Members', header_format)
        worksheet.write(current_row, left_col + 7, 'Wrong Gender Ratio', header_format)
        current_row += 1
        
        for mismatch in ticket_status_data['mixed_mismatches']:
            if mismatch['details']:
                details = mismatch['details']
                if isinstance(details, str):
                    details = json.loads(details)
                
                for detail in details:
                    worksheet.write(current_row, left_col, mismatch['ticket_name'], warning_format)
                    worksheet.write(current_row, left_col + 1, detail['transaction_id'], warning_format)
                    worksheet.write(current_row, left_col + 2, detail['main_barcode'], warning_format)
                    worksheet.write(current_row, left_col + 3, detail['main_gender'], warning_format)
                    worksheet.write(current_row, left_col + 4, detail['partner_barcode'], warning_format)
                    worksheet.write(current_row, left_col + 5, detail['partner_gender'], warning_format)
                    worksheet.write(current_row, left_col + 6, 'Yes' if detail['has_wrong_member_count'] else 'No', warning_format)
                    worksheet.write(current_row, left_col + 7, 'Yes' if detail['has_wrong_gender_ratio'] else 'No', warning_format)
                    current_row += 1
        
        # Right side content
        right_col = 9  # Added one column space for separation
        current_row = 2
        
        # 1. Sportograf Summary
        worksheet.merge_range(current_row, right_col, current_row, right_col + 1, 'Sportograf Package Summary', section_format)
        current_row += 1
        
        worksheet.write(current_row, right_col, 'Package Type', header_format)
        worksheet.write(current_row, right_col + 1, 'Count', header_format)
        current_row += 1
        
        for sportograf in ticket_status_data['sportograf_data']:
            worksheet.write(current_row, right_col, sportograf['ticket_name'], data_format)
            worksheet.write(current_row, right_col + 1, sportograf['count'], number_format)
            current_row += 1
        
        current_row += 2
        
        # 2. Age Restricted Athletes (17-18)
        worksheet.merge_range(current_row, right_col, current_row, right_col + 3, 'Athletes Age 17-18', section_format)
        current_row += 1
        
        worksheet.write(current_row, right_col, 'Barcode', header_format)
        worksheet.write(current_row, right_col + 1, 'Ticket Type', header_format)
        worksheet.write(current_row, right_col + 2, 'Category', header_format)
        worksheet.write(current_row, right_col + 3, 'Age', header_format)
        current_row += 1
        
        for athlete in ticket_status_data['age_restricted']['17_to_18']:
            worksheet.write(current_row, right_col, athlete['barcode'], warning_format)
            worksheet.write(current_row, right_col + 1, athlete['ticket_name'], warning_format)
            worksheet.write(current_row, right_col + 2, athlete['category_name'], warning_format)
            worksheet.write(current_row, right_col + 3, athlete['age'], warning_format)
            current_row += 1
        
        current_row += 2
        
        # 3. Age Restricted Athletes (Under 16 or 16)
        worksheet.merge_range(current_row, right_col, current_row, right_col + 3, 'Athletes Under 16 or 16', section_format)
        current_row += 1
        
        worksheet.write(current_row, right_col, 'Barcode', header_format)
        worksheet.write(current_row, right_col + 1, 'Ticket Type', header_format)
        worksheet.write(current_row, right_col + 2, 'Category', header_format)
        worksheet.write(current_row, right_col + 3, 'Age', header_format)
        current_row += 1
        
        for athlete in ticket_status_data['age_restricted']['under_16']:
            worksheet.write(current_row, right_col, athlete['barcode'], warning_format)
            worksheet.write(current_row, right_col + 1, athlete['ticket_name'], warning_format)
            worksheet.write(current_row, right_col + 2, athlete['category_name'], warning_format)
            worksheet.write(current_row, right_col + 3, athlete['age'], warning_format)
            current_row += 1
        
        # Set column widths
        # Left side
        worksheet.set_column(left_col, left_col, 40)  # Main ticket name
        worksheet.set_column(left_col + 1, left_col + 2, 15)  # Count columns
        worksheet.set_column(left_col + 3, left_col + 3, 12)  # Category
        worksheet.set_column(left_col + 4, left_col + 4, 12)  # Event Day
        worksheet.set_column(left_col + 5, left_col + 5, 12)  # Status
        
        # Separator column
        worksheet.set_column(8, 8, 2)  # Small gap between left and right sections
        
        # Right side
        worksheet.set_column(right_col, right_col, 15)  # Barcode
        worksheet.set_column(right_col + 1, right_col + 1, 40)  # Ticket Type
        worksheet.set_column(right_col + 2, right_col + 2, 25)  # Category
        worksheet.set_column(right_col + 3, right_col + 3, 10)  # Age/Count
        
        # Freeze panes
        worksheet.freeze_panes(1, 0)  # Freeze after event info

class Analytics:
    """Main analytics coordinator"""

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
                # Generate and send Excel only
                excel_path = self.excel_generator.create_report(
                    age_group_data,
                    event_info,
                    self.schema,
                    self.region
                )
                results.append(bool(excel_path))
                
                if send_slack and excel_path:
                    # Define a mapping of regions to icons
                    icon_mapping = self.load_icon_mapping()
                    # Get the icon based on the schema (which is the region)
                    icon = icon_mapping.get(self.region, icon_mapping["default"])
                    success = self.slack_service.send_excel_report(
                        excel_path,
                        f"{icon} {event_info.get('name', 'Event')} Report"
                    )
                    results.append(success)
            elif send_slack:
                # Send formatted message to Slack only if Excel is not requested
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