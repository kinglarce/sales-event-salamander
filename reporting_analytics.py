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

    def get_returning_athletes_data(self) -> Dict[str, int]:
        """Get counts of returning athletes"""
        try:
            query = f"""
                SELECT 
                    SUM(CASE WHEN is_returning_athlete = true THEN 1 ELSE 0 END) as returning_athletes,
                    SUM(CASE WHEN is_returning_athlete_to_city = true THEN 1 ELSE 0 END) as returning_to_city
                FROM {self.schema}.tickets
            """
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
        """Get region of residence distribution"""
        try:
            query = f"""
                SELECT 
                    CASE 
                        WHEN t.region_of_residence IN (
                            SELECT code FROM {self.schema}.country_configs
                        ) THEN (
                            SELECT country 
                            FROM {self.schema}.country_configs 
                            WHERE code = t.region_of_residence
                        )
                        ELSE t.region_of_residence
                    END as region,
                    COUNT(*) as count
                FROM {self.schema}.tickets t
                WHERE t.region_of_residence IS NOT NULL
                GROUP BY t.region_of_residence
                ORDER BY count DESC
            """
            results = self.db.execute_query(query)
            return pd.DataFrame(results, columns=['region', 'count'])
        except Exception as e:
            logger.error(f"Error getting region of residence data: {e}")
            return pd.DataFrame()

    def get_gym_affiliate_data(self) -> Dict[str, Any]:
        """Get gym affiliate statistics"""
        try:
            # Get all gym affiliate details with pattern-based ordering and country names
            member_details_query = f"""
                SELECT 
                    t.is_gym_affiliate as membership_type,
                    COALESCE(t.gym_affiliate, 'Not Specified') as gym,
                    CASE 
                        WHEN t.gym_affiliate_location IN (
                            SELECT code FROM {self.schema}.country_configs
                        ) THEN (
                            SELECT country 
                            FROM {self.schema}.country_configs 
                            WHERE code = t.gym_affiliate_location
                        )
                        ELSE COALESCE(t.gym_affiliate_location, 'Not Specified')
                    END as location,
                    COUNT(*) as count
                FROM {self.schema}.tickets t
                WHERE t.is_gym_affiliate IS NOT NULL
                GROUP BY 
                    t.is_gym_affiliate,
                    t.gym_affiliate,
                    t.gym_affiliate_location
                ORDER BY 
                    CASE 
                        WHEN t.is_gym_affiliate LIKE 'I''m a member of%' AND 
                             t.is_gym_affiliate NOT LIKE 'I''m a member of another%' THEN 1
                        WHEN t.is_gym_affiliate LIKE 'I''m a member of another%' THEN 2
                        WHEN t.is_gym_affiliate LIKE 'I''m not a member%' THEN 3
                        ELSE 4
                    END,
                    t.is_gym_affiliate,
                    count DESC
            """
            member_details = self.db.execute_query(member_details_query)
            logger.info(f"Found {len(member_details)} gym affiliate details")
            
            # Process the results to get unique values and membership counts
            membership_counts = {}
            seen_types = set()
            ordered_unique_values = []
            
            # Convert results to the required format and calculate totals
            member_details_list = []
            for row in member_details:
                membership_type = row[0]
                
                # Add to unique values list if not seen before
                if membership_type not in seen_types:
                    ordered_unique_values.append(membership_type)
                    seen_types.add(membership_type)
                
                # Add to membership counts
                membership_counts[membership_type] = membership_counts.get(membership_type, 0) + row[3]
                
                # Add to member details list
                member_details_list.append({
                    'membership_type': membership_type,
                    'gym': row[1],
                    'location': row[2],
                    'count': row[3]
                })
            
            logger.info(f"Found unique membership types (ordered): {ordered_unique_values}")
            logger.info(f"Membership counts: {membership_counts}")
            
            return {
                'unique_values': ordered_unique_values,
                'membership_counts': membership_counts,
                'member_details': member_details_list
            }
        except Exception as e:
            logger.error(f"Error getting gym affiliate data: {e}")
            return {'unique_values': [], 'membership_counts': {}, 'member_details': []}

    def get_ticket_status_data(self) -> Dict[str, Any]:
        """Get ticket status data including status counts, team member counts, gender mismatches, and sportograf data"""
        try:
            # 1. Get ticket status counts (excluding spectator and extra categories)
            status_query = f"""
                SELECT 
                    t.status,
                    COUNT(*) as count
                FROM {self.schema}.tickets t
                JOIN {self.schema}.ticket_type_summary tt ON t.ticket_type_id = tt.ticket_type_id
                WHERE tt.ticket_category NOT IN ('spectator', 'extra')
                GROUP BY t.status
                ORDER BY t.status
            """
            status_results = self.db.execute_query(status_query)
            status_counts = {row[0]: row[1] for row in status_results}
            
            # 2. Get team member counts for doubles and relays with main ticket info
            team_query = f"""
                WITH main_tickets AS (
                    SELECT 
                        tt.ticket_name,
                        tt.total_count as main_count,
                        tt.ticket_category,
                        CASE 
                            WHEN LOWER(tt.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(tt.ticket_name), ' | ', 1)
                            ELSE LOWER(tt.ticket_name)
                        END as base_name
                    FROM {self.schema}.ticket_type_summary tt
                    WHERE tt.ticket_category IN ('double', 'relay')
                    AND NOT (
                            tt.ticket_name LIKE '%ATHLETE 2%'
                            OR tt.ticket_name LIKE '%ATHLETE2%'
                            OR tt.ticket_name LIKE '%TEAM MEMBER%'
                            OR tt.ticket_name LIKE '%MEMBER%'
                        )
                    ),
                member_tickets AS (
                    SELECT 
                        member_ticket_name,
                        member_count,
                        CASE 
                            WHEN LOWER(member_ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(member_ticket_name), ' | ', 1)
                            ELSE LOWER(member_ticket_name)
                        END as base_name
                    FROM (
                            SELECT 
                                CASE
                                    WHEN tt.ticket_name LIKE '%ATHLETE 2%' OR tt.ticket_name LIKE '%ATHLETE2%' THEN 
                                        SPLIT_PART(tt.ticket_name, ' ATHLETE', 1)
                                    WHEN tt.ticket_name LIKE '%TEAM MEMBER%' THEN 
                                        SPLIT_PART(tt.ticket_name, ' TEAM MEMBER', 1)
                                    WHEN tt.ticket_name LIKE '%MEMBER%' THEN 
                                        SPLIT_PART(tt.ticket_name, ' MEMBER', 1)
                                END as member_ticket_name,
                                tt.total_count as member_count
                            FROM {self.schema}.ticket_type_summary tt
                            WHERE tt.ticket_name LIKE '%ATHLETE 2%'
                                OR tt.ticket_name LIKE '%ATHLETE2%'
                                OR tt.ticket_name LIKE '%TEAM MEMBER%'
                                OR tt.ticket_name LIKE '%MEMBER%'
                        )
                )
                SELECT 
                m.ticket_name as main_ticket_name,
                m.main_count,
                COALESCE(t.member_count, 0) as member_count,
                m.ticket_category,
                CASE 
                    WHEN m.ticket_category = 'relay' AND COALESCE(t.member_count, 0) = m.main_count * 3 THEN 'OK'
                    WHEN m.ticket_category = 'double' AND COALESCE(t.member_count, 0) = m.main_count THEN 'OK'
                    ELSE 'MISMATCH'
                END as status
                FROM main_tickets m
                LEFT JOIN member_tickets t ON t.base_name = m.base_name
                ORDER BY 
                m.ticket_category,
                m.ticket_name
            """
            team_results = self.db.execute_query(team_query)
            team_counts = [
                {
                    'main_ticket_name': row[0],
                    'main_count': row[1],
                    'member_count': row[2],
                    'ticket_category': row[3],
                    'status': row[4]
                }
                for row in team_results
            ]
            
            # 3. Get gender mismatches (excluding mixed categories)
            gender_query = f"""
                WITH gender_mismatch_base AS (
                    SELECT 
                        t.ticket_name,
                        t.gender,
                        COUNT(*) as count
                    FROM {self.schema}.tickets t
                    JOIN {self.schema}.ticket_type_summary tt ON t.ticket_type_id = tt.ticket_type_id
                    WHERE (
                        (t.ticket_name LIKE '%WOMEN%' AND t.gender = 'Male')
                        OR (t.ticket_name LIKE '%MEN%' AND NOT t.ticket_name LIKE '%WOMEN%' AND t.gender = 'Female')
                    )
                    AND NOT t.ticket_name LIKE '%MIXED%'
                    GROUP BY t.ticket_name, t.gender
                ),
                gender_mismatch_details AS (
                    SELECT 
                        t.ticket_name,
                        t.gender,
                        t.barcode,
                        t.ticket_type_id,
                        t.category_name
                    FROM {self.schema}.tickets t
                    JOIN gender_mismatch_base g ON t.ticket_name = g.ticket_name AND t.gender = g.gender
                )
                SELECT 
                    b.ticket_name,
                    b.gender,
                    b.count,
                    json_agg(json_build_object(
                        'barcode', d.barcode,
                        'ticket_type_id', d.ticket_type_id,
                        'category_name', d.category_name
                    )) as details
                FROM gender_mismatch_base b
                JOIN gender_mismatch_details d ON b.ticket_name = d.ticket_name AND b.gender = d.gender
                GROUP BY b.ticket_name, b.gender, b.count
            """
            gender_results = self.db.execute_query(gender_query)
            gender_mismatches = [
                {
                    'ticket_name': row[0],
                    'gender': row[1],
                    'count': row[2],
                    'details': row[3]
                }
                for row in gender_results
            ]
            
            # 4. Get mixed pairing mismatches with optimized query
            mixed_query = f"""
                WITH mixed_pairs AS (
                    SELECT 
                        t.transaction_id,
                        t.ticket_name,
                        t.barcode,
                        t.gender,
                        t.category_name,
                        CASE 
                            WHEN t.ticket_name LIKE '%ATHLETE 2%' OR t.ticket_name LIKE '%TEAM MEMBER%' 
                            THEN 'MEMBER' 
                            ELSE 'MAIN' 
                        END as ticket_type,
                        -- Number tickets within their type (MAIN/MEMBER) for each transaction
                        ROW_NUMBER() OVER (
                            PARTITION BY t.transaction_id, 
                                CASE 
                                    WHEN t.ticket_name LIKE '%ATHLETE 2%' OR t.ticket_name LIKE '%TEAM MEMBER%' 
                                    THEN 'MEMBER' 
                                    ELSE 'MAIN' 
                                END
                            ORDER BY t.ticket_name
                        ) as pair_number
                    FROM {self.schema}.tickets t
                    WHERE t.ticket_name LIKE '%MIXED%'
                ),
                paired_tickets AS (
                    SELECT 
                        m.transaction_id,
                        m.ticket_name,  -- Keep original ticket_name
                        m.ticket_type,  -- Keep ticket_type separate
                        m.barcode as main_barcode,
                        m.gender as main_gender,
                        m.category_name as main_category,
                        p.barcode as partner_barcode,
                        p.gender as partner_gender,
                        p.category_name as partner_category
                    FROM mixed_pairs m
                    LEFT JOIN mixed_pairs p ON 
                        m.transaction_id = p.transaction_id AND
                        m.pair_number = p.pair_number AND
                        m.ticket_type = 'MAIN' AND 
                        p.ticket_type = 'MEMBER'
                    WHERE m.ticket_type = 'MAIN'
                ),
                invalid_pairs AS (
                    SELECT 
                        pt.*,
                        CASE
                            WHEN pt.ticket_name LIKE '%MIXED RELAY%' 
                            THEN (
                                SELECT COUNT(*) 
                                FROM mixed_pairs mp 
                                WHERE mp.transaction_id = pt.transaction_id
                                AND mp.ticket_type = 'MEMBER'
                            ) != 3
                            WHEN pt.ticket_name LIKE '%MIXED DOUBLES%' 
                            THEN (
                                SELECT COUNT(*) 
                                FROM mixed_pairs mp 
                                WHERE mp.transaction_id = pt.transaction_id
                                AND mp.ticket_type = 'MEMBER'
                            ) != 1
                            ELSE false
                        END as has_wrong_member_count,
                        CASE
                            WHEN pt.ticket_name LIKE '%MIXED RELAY%' 
                            THEN (
                                SELECT COUNT(*) 
                                FROM mixed_pairs mp 
                                WHERE mp.transaction_id = pt.transaction_id
                                AND mp.gender = 'Male'
                            ) != 2 OR
                                (
                                SELECT COUNT(*) 
                                FROM mixed_pairs mp 
                                WHERE mp.transaction_id = pt.transaction_id
                                AND mp.gender = 'Female'
                            ) != 2
                            WHEN pt.ticket_name LIKE '%MIXED DOUBLES%' 
                            THEN (
                                SELECT COUNT(*) 
                                FROM mixed_pairs mp 
                                WHERE mp.transaction_id = pt.transaction_id
                                AND mp.gender = 'Male'
                            ) != 1 OR
                                (
                                SELECT COUNT(*) 
                                FROM mixed_pairs mp 
                                WHERE mp.transaction_id = pt.transaction_id
                                AND mp.gender = 'Female'
                            ) != 1
                            ELSE false
                        END as has_wrong_gender_ratio
                    FROM paired_tickets pt
                    WHERE pt.ticket_name LIKE '%MIXED%'
                )
                SELECT 
                    ticket_name,
                    COUNT(*) as invalid_count,
                    json_agg(json_build_object(
                        'transaction_id', transaction_id,
                        'main_barcode', main_barcode,
                        'main_gender', main_gender,
                        'main_category', main_category,
                        'partner_barcode', partner_barcode,
                        'partner_gender', partner_gender,
                        'partner_category', partner_category,
                        'has_wrong_member_count', has_wrong_member_count,
                        'has_wrong_gender_ratio', has_wrong_gender_ratio
                    )) as details
                FROM invalid_pairs
                WHERE has_wrong_member_count OR has_wrong_gender_ratio
                GROUP BY ticket_name
            """
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
            age_query = f"""
                WITH age_restricted_athletes AS (
                    SELECT 
                        CASE 
                            WHEN t.age <= 16 THEN 'under_16'
                            WHEN t.age >= 17 AND t.age <= 18 THEN '17_to_18'
                        END as age_group,
                        json_agg(json_build_object(
                            'barcode', t.barcode,
                            'ticket_name', t.ticket_name,
                            'ticket_type_id', t.ticket_type_id,
                            'category_name', t.category_name,
                            'age', t.age
                        ) ORDER BY t.age) as athletes
                    FROM {self.schema}.tickets t
                    JOIN {self.schema}.ticket_type_summary tt ON t.ticket_type_id = tt.ticket_type_id
                    WHERE t.age <= 18
                      AND tt.ticket_category NOT IN ('spectator', 'extra')
                    GROUP BY age_group
                )
                SELECT age_group, athletes
                FROM age_restricted_athletes
            """
            age_results = self.db.execute_query(age_query)
            age_restricted = {
                'under_16': [],
                '17_to_18': []
            }
            for row in age_results:
                if row[0]:  # age_group is not None
                    age_restricted[row[0]] = row[1]
            
            # 6. Get sportograf data
            sportograf_query = f"""
                SELECT 
                    tt.ticket_name,
                    tt.total_count
                FROM {self.schema}.ticket_type_summary tt
                WHERE tt.ticket_name LIKE '%Sportograf%'
                ORDER BY tt.ticket_name
            """
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
            # Group ticket groups by category
            singles = [g for g in df['ticket_group'].unique() if 'DOUBLES' not in g and 'RELAY' not in g]
            doubles = [g for g in df['ticket_group'].unique() if 'DOUBLES' in g]
            relays = [g for g in df['ticket_group'].unique() if 'RELAY' in g]

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

            # Process each category separately
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
        
        # Get appropriate age ranges based on first group's category
        first_group = groups[0]
        if 'DOUBLES' in first_group:
            age_ranges = ['U29', '30-39', '40-49', '50-59', '60-69', '70+', 'Incomplete', 'Total']
        elif 'RELAY' in first_group:
            age_ranges = ['U40', '40+', 'Incomplete', 'Total']
        else:  # Singles
            age_ranges = ['U24', '25-29', '30-34', '35-39', '40-44', '45-49', 
                         '50-54', '55-59', '60-64', '65-69', '70+', 'Incomplete', 'Total']
        
        # Data rows
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
    
    @staticmethod
    def get_age_ranges_for_category(category: str) -> List[str]:
        if 'DOUBLES' in category:
            return ['U29', '30-39', '40-49', '50-59', '60-69', '70+', 'Incomplete', 'Total']
        elif 'RELAY' in category:
            return ['U40', '40+', 'Incomplete', 'Total']
        else:  # Singles
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
        
        # Get unique ticket groups and age ranges
        ticket_groups = sorted(df['ticket_group'].unique())
        
        current_row = 4
        max_col = 0
        
        # Define categories and their ticket groups
        categories = {
            'SINGLES': ['HYROX MEN', 'HYROX WOMEN', 'HYROX PRO MEN', 'HYROX PRO WOMEN',
                         'HYROX ADAPTIVE MEN', 'HYROX ADAPTIVE WOMEN'],
            'DOUBLES': ['HYROX DOUBLES MEN', 'HYROX DOUBLES WOMEN', 'HYROX DOUBLES MIXED',
                       'HYROX PRO DOUBLES MEN', 'HYROX PRO DOUBLES WOMEN'],
            'RELAY': ['HYROX MENS RELAY', 'HYROX WOMENS RELAY', 'HYROX MIXED RELAY'],
            'CORPORATE RELAY': ['HYROX MENS CORPORATE RELAY', 'HYROX WOMENS CORPORATE RELAY',
                              'HYROX MIXED CORPORATE RELAY']
        }

        # Write data for each category
        for category, groups in categories.items():
            existing_groups = [g for g in groups if g in ticket_groups]
            if not existing_groups:
                continue
            
            # Get appropriate age ranges for this category
            age_ranges = self.get_age_ranges_for_category(category)
                
            # Write category header
            worksheet.merge_range(current_row, 0, current_row, len(age_ranges), category, section_format)
            current_row += 1
            
            # Write age range headers
            worksheet.write(current_row, 0, "Age Range", header_format)
            for col, age_range in enumerate(age_ranges, 1):
                worksheet.write(current_row, col, age_range, header_format)
            current_row += 1
            
            # Write data for each group
            for group in existing_groups:
                worksheet.write(current_row, 0, group, category_format)
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
        worksheet.merge_range(current_row, left_col, current_row, 4, 'Team Member Count Verification', section_format)
        current_row += 1
        
        # Headers
        worksheet.write(current_row, left_col, 'Main Ticket', header_format)
        worksheet.write(current_row, left_col + 1, 'Main Count', header_format)
        worksheet.write(current_row, left_col + 2, 'Member Count', header_format)
        worksheet.write(current_row, left_col + 3, 'Category', header_format)
        worksheet.write(current_row, left_col + 4, 'Status', header_format)
        current_row += 1
        
        for team_count in ticket_status_data['team_counts']:
            worksheet.write(current_row, left_col, team_count['main_ticket_name'], data_format)
            worksheet.write(current_row, left_col + 1, team_count['main_count'], number_format)
            worksheet.write(current_row, left_col + 2, team_count['member_count'], number_format)
            worksheet.write(current_row, left_col + 3, team_count['ticket_category'].upper(), data_format)
            
            # Status formatting
            format_to_use = warning_format if team_count['status'] != 'OK' else data_format
            worksheet.write(current_row, left_col + 4, team_count['status'], format_to_use)
            current_row += 1
        
        # Set column widths
        worksheet.set_column(left_col, left_col, 40)      # Main ticket name
        worksheet.set_column(left_col + 1, left_col + 2, 15)  # Count columns
        worksheet.set_column(left_col + 3, left_col + 3, 12)  # Category
        worksheet.set_column(left_col + 4, left_col + 4, 12)  # Status
        
        current_row += 2
        
        # 3. Gender Mismatch Report
        worksheet.merge_range(current_row, left_col, current_row, 2, 'Gender Mismatch Report', section_format)
        current_row += 1
        
        # Summary table
        worksheet.write(current_row, left_col, 'Ticket Type', header_format)
        worksheet.write(current_row, left_col + 1, 'Gender', header_format)
        worksheet.write(current_row, left_col + 2, 'Count', header_format)
        current_row += 1
        
        for mismatch in ticket_status_data['gender_mismatches']:
            worksheet.write(current_row, left_col, mismatch['ticket_name'], warning_format)
            worksheet.write(current_row, left_col + 1, mismatch['gender'], warning_format)
            worksheet.write(current_row, left_col + 2, mismatch['count'], warning_format)
            current_row += 1
        
        current_row += 2
        
        # Gender Mismatch Detailed Report
        worksheet.merge_range(current_row, left_col, current_row, 3, 'Gender Mismatch Detailed Report', section_format)
        current_row += 1
        
        worksheet.write(current_row, left_col, 'Barcode', header_format)
        worksheet.write(current_row, left_col + 1, 'Ticket Type', header_format)
        worksheet.write(current_row, left_col + 2, 'Category', header_format)
        worksheet.write(current_row, left_col + 3, 'Gender', header_format)
        current_row += 1
        
        for mismatch in ticket_status_data['gender_mismatches']:
            for detail in mismatch['details']:
                worksheet.write(current_row, left_col, detail['barcode'], warning_format)
                worksheet.write(current_row, left_col + 1, mismatch['ticket_name'], warning_format)
                worksheet.write(current_row, left_col + 2, detail['category_name'], warning_format)
                worksheet.write(current_row, left_col + 3, mismatch['gender'], warning_format)
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
        worksheet.set_column(left_col + 4, left_col + 4, 12)  # Status
        
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