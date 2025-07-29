import os
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from typing import Dict, List, Optional, Any
import argparse
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import pytz
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

if os.getenv('ENABLE_FILE_LOGGING', 'true').strip().lower() in ('true', '1'):
    log_filename = f'logs/coupon_analytics_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(file_handler)

logger = logging.getLogger(__name__)

class SlackService:
    """Handles Slack communication for coupon reports"""
    
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

    def send_report(self, df: pd.DataFrame, untracked_df: pd.DataFrame = None) -> bool:
        """Send formatted coupon report to Slack"""
        if not self.client or not self.channel_id:
            return False

        try:
            blocks = []
            icon_mapping = self._load_icon_mapping()
            icon = icon_mapping.get(self.region, icon_mapping["default"])
            
            blocks.append({
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{icon} {self.schema.upper()} Coupon Usage Report"
                }
            })

            # Add summary statistics
            if not df.empty:
                total_tracked = df['tracked_codes'].sum()
                total_used = df['tracked_used_codes'].sum()
                usage_rate = (total_used / total_tracked * 100) if total_tracked > 0 else 0
                
                summary_text = (
                    f"*Summary:*\n"
                    f"• Total Tracked Codes: {total_tracked}\n"
                    f"• Total Used: {total_used}\n"
                    f"• Usage Rate: {usage_rate:.1f}%"
                )
                
                # Add untracked used codes count if available
                if untracked_df is not None and not untracked_df.empty:
                    summary_text += f"\n• ⚠️ Untracked Used Codes: {len(untracked_df)}"
                
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": summary_text
                    }
                })

                # Add series breakdown
                if len(df) > 0:
                    table_text = self._create_summary_table(df)
                    blocks.append({
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": table_text
                        }
                    })
            
            # Add untracked used codes section if available
            if untracked_df is not None and not untracked_df.empty:
                blocks.append({"type": "divider"})
                
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*⚠️ Untracked Used Codes ({len(untracked_df)}):*"
                    }
                })
                
                untracked_table = self._create_untracked_table(untracked_df)
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": untracked_table
                    }
                })
            
            response = self.client.chat_postMessage(
                channel=self.channel_id,
                blocks=blocks,
                text=f"{self.schema.upper()} Coupon Usage Report"
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

    def _load_icon_mapping(self) -> Dict:
        try:
            with open("icons.json", "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"default": "🎟️"}

    def _create_summary_table(self, df: pd.DataFrame) -> str:
        """Create formatted table text for Slack message"""
        table_text = "```\n"
        table_text += f"{'Series':<25} | {'Tracked':>8} | {'Used':>8} | {'Rate':>8}\n"
        table_text += f"{'-'*25} | {'-'*8} | {'-'*8} | {'-'*8}\n"
        
        for _, row in df.iterrows():
            series_name = row['series_name'][:24] if row['series_name'] else 'Unknown'
            tracked = row['tracked_codes']
            used = row['tracked_used_codes']
            rate = row['tracked_usage_percentage']
            
            table_text += f"{series_name:<25} | {tracked:>8} | {used:>8} | {rate:>8}\n"
        
        table_text += "```"
        return table_text
    
    def _create_untracked_table(self, df: pd.DataFrame) -> str:
        """Create formatted table text for untracked used codes"""
        table_text = "```\n"
        table_text += f"{'Code':<20} | {'Series':<20} | {'Usage Count':>12}\n"
        table_text += f"{'-'*20} | {'-'*20} | {'-'*12}\n"
        
        for _, row in df.iterrows():
            code = row['code'][:19] if row['code'] else 'Unknown'
            series_name = row['series_name'][:19] if row['series_name'] else 'Unknown'
            usage_count = row['usage_count']
            
            table_text += f"{code:<20} | {series_name:<20} | {usage_count:>12}\n"
        
        table_text += "```"
        return table_text

class CouponAnalytics:
    """Handles coupon analytics and reporting"""
    
    def __init__(self, schema: str, region: Optional[str] = None):
        self.schema = schema
        self.region = region
        db_url = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        self.engine = create_engine(db_url)
        
        # Initialize Slack service if region is provided
        self.slack_service = SlackService(schema, region) if region else None
        
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List:
        """Execute a SQL query with parameters"""
        try:
            with self.engine.connect() as conn:
                query_text = text(query.replace('{SCHEMA}', self.schema))
                result = conn.execute(query_text, params or {})
                return result.fetchall()
        except Exception as e:
            logger.error(f"Database query error: {e}")
            return []
    
    def get_coupon_usage_report(self) -> pd.DataFrame:
        """Get coupon usage summary report"""
        try:
            with open('sql/get_coupon_usage_report.sql', 'r') as file:
                sql_query = file.read()
            
            results = self.execute_query(sql_query)
            
            if not results:
                logger.warning("No coupon usage data found")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(results, columns=[
                'series_name', 'total_codes', 'used_codes', 'unused_codes',
                'tracked_codes', 'tracked_used_codes', 'tracked_unused_codes',
                'tracked_usage_percentage'
            ])
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting coupon usage report: {e}")
            return pd.DataFrame()
    
    def get_distributed_codes_status(self) -> pd.DataFrame:
        """Get detailed status of distributed codes"""
        try:
            with open('sql/get_distributed_codes_status.sql', 'r') as file:
                sql_query = file.read()
            
            results = self.execute_query(sql_query)
            
            if not results:
                logger.warning("No distributed codes found")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(results, columns=[
                'code', 'series_name', 'category', 'is_used', 'usage_count', 'status'
            ])
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting distributed codes status: {e}")
            return pd.DataFrame()
    
    def get_untracked_used_codes(self) -> pd.DataFrame:
        """Get codes that are used but not in the distributed CSV"""
        try:
            with open('sql/get_untracked_used_codes.sql', 'r') as file:
                sql_query = file.read()
            
            results = self.execute_query(sql_query)
            
            if not results:
                logger.warning("No untracked used codes found")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(results, columns=[
                'code', 'series_name', 'is_used', 'usage_count', 'status'
            ])
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting untracked used codes: {e}")
            return pd.DataFrame()
    
    def get_distributed_codes_paired_report(self) -> pd.DataFrame:
        """Get paired report that matches your distributed CSV exactly"""
        try:
            with open('sql/get_distributed_codes_paired_report.sql', 'r') as file:
                sql_query = file.read()
            
            results = self.execute_query(sql_query)
            
            if not results:
                logger.warning("No distributed codes found for paired report")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(results, columns=[
                'code', 'series_name', 'is_used', 'usage_count', 'status', 'status_icon'
            ])
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting distributed codes paired report: {e}")
            return pd.DataFrame()
    
    def generate_excel_report(self, output_path: Optional[str] = None) -> str:
        """Generate Excel report with coupon analytics"""
        if not output_path:
            # Create excels directory if it doesn't exist
            os.makedirs('excels', exist_ok=True)
            
            # Use proper naming convention: {REGION}_coupon_yyyymmdd.xlsx
            region_upper = self.region.upper() if self.region else self.schema.upper()
            date_str = datetime.now().strftime("%Y%m%d")
            output_path = f"excels/{region_upper}_coupon_{date_str}.xlsx"
        
        try:
            # Try openpyxl first, fallback to xlsxwriter
            try:
                with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                    self._write_excel_sheets(writer)
            except ImportError:
                logger.warning("openpyxl not available, using xlsxwriter")
                with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                    self._write_excel_sheets(writer)
            
            logger.info(f"Excel report generated: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error generating Excel report: {e}")
            raise
    
    def _write_excel_sheets(self, writer):
        """Write all sheets to the Excel file"""
        # Usage summary sheet
        usage_df = self.get_coupon_usage_report()
        if not usage_df.empty:
            usage_df.to_excel(writer, sheet_name='Usage Summary', index=False)
            logger.info(f"Added usage summary with {len(usage_df)} series")
        
        # Distributed codes status sheet
        codes_df = self.get_distributed_codes_status()
        if not codes_df.empty:
            codes_df.to_excel(writer, sheet_name='Distributed Codes', index=False)
            logger.info(f"Added distributed codes with {len(codes_df)} codes")
        
        # Untracked used codes sheet
        untracked_df = self.get_untracked_used_codes()
        if not untracked_df.empty:
            untracked_df.to_excel(writer, sheet_name='Untracked Used Codes', index=False)
            logger.info(f"Added untracked used codes with {len(untracked_df)} codes")
        
        # Summary statistics sheet
        if not usage_df.empty:
            summary_stats = {
                'Metric': [
                    'Total Series with Tracked Codes',
                    'Total Tracked Codes',
                    'Total Used Tracked Codes',
                    'Total Unused Tracked Codes',
                    'Overall Usage Rate',
                    'Untracked Used Codes'
                ],
                'Value': [
                    len(usage_df),
                    usage_df['tracked_codes'].sum(),
                    usage_df['tracked_used_codes'].sum(),
                    usage_df['tracked_unused_codes'].sum(),
                    f"{(usage_df['tracked_used_codes'].sum() / usage_df['tracked_codes'].sum() * 100):.2f}%" if usage_df['tracked_codes'].sum() > 0 else "0%",
                    len(untracked_df) if not untracked_df.empty else 0
                ]
            }
            summary_df = pd.DataFrame(summary_stats)
            summary_df.to_excel(writer, sheet_name='Summary Stats', index=False)
            logger.info("Added summary statistics")
    
    def print_summary(self):
        """Print a summary of coupon usage to console"""
        try:
            usage_df = self.get_coupon_usage_report()
            untracked_df = self.get_untracked_used_codes()
            
            if usage_df.empty and untracked_df.empty:
                print("No coupon usage data found.")
                return
            
            print(f"\n=== Coupon Usage Summary for {self.schema} ===")
            print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 60)
            
            if not usage_df.empty:
                total_tracked = usage_df['tracked_codes'].sum()
                total_used = usage_df['tracked_used_codes'].sum()
                total_unused = usage_df['tracked_unused_codes'].sum()
                
                print(f"Total Tracked Codes: {total_tracked}")
                print(f"Total Used: {total_used}")
                print(f"Total Unused: {total_unused}")
                print(f"Overall Usage Rate: {(total_used / total_tracked * 100):.2f}%" if total_tracked > 0 else "0%")
                
                print(f"\nBreakdown by Series:")
                print("-" * 60)
                for _, row in usage_df.iterrows():
                    print(f"{row['series_name']}:")
                    print(f"  Tracked: {row['tracked_codes']}, Used: {row['tracked_used_codes']}, Rate: {row['tracked_usage_percentage']}")
            
            # Add untracked used codes section
            if not untracked_df.empty:
                print(f"\n⚠️  UNTRACKED USED CODES ({len(untracked_df)}):")
                print("-" * 60)
                for _, row in untracked_df.iterrows():
                    print(f"Code: {row['code']} | Series: {row['series_name']} | Usage: {row['usage_count']}")
            
            print("\n" + "=" * 60)
            
        except Exception as e:
            logger.error(f"Error printing summary: {e}")
    
    def send_slack_report(self) -> bool:
        """Send coupon usage report to Slack"""
        if not self.slack_service:
            logger.warning("Slack service not initialized")
            return False
        
        try:
            usage_df = self.get_coupon_usage_report()
            untracked_df = self.get_untracked_used_codes()
            
            if usage_df.empty and untracked_df.empty:
                logger.warning("No data to send to Slack")
                return False
            
            success = self.slack_service.send_report(usage_df, untracked_df)
            return success
            
        except Exception as e:
            logger.error(f"Error sending Slack report: {e}")
            return False
    
    def send_excel_to_slack(self, file_path: str) -> bool:
        """Send Excel file to Slack"""
        if not self.slack_service:
            logger.warning("Slack service not initialized")
            return False
        
        try:
            # Define a mapping of regions to icons
            icon_mapping = self._load_icon_mapping()
            # Get the icon based on the region
            icon = icon_mapping.get(self.region, icon_mapping["default"])
            
            message = f"{icon} {self.schema.upper()} Coupon Usage Report"
            success = self.slack_service.send_excel_report(file_path, message)
            return success
            
        except Exception as e:
            logger.error(f"Error sending Excel to Slack: {e}")
            return False
    
    def _load_icon_mapping(self) -> Dict:
        try:
            with open("icons.json", "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"default": "🎟️"}
    
    def close(self):
        """Close database connection"""
        self.engine.dispose()

def get_event_configs():
    """Get all event configurations from environment"""
    from collections import defaultdict
    
    configs = defaultdict(dict)
    for key, value in os.environ.items():
        if key.startswith("EVENT_CONFIGS__"):
            _, region, param = key.split("__", 2)
            if param in ["token", "event_id", "schema_name", "REPORTING_CHANNEL"]:
                configs[region][param] = value
            configs[region]["region"] = region

    return [
        {
            "token": config["token"],
            "event_id": config["event_id"],
            "schema": config["schema_name"],
            "region": config["region"],
            "reporting_channel": config.get("REPORTING_CHANNEL", "default")
        }
        for config in configs.values()
        if all(k in config for k in ["token", "event_id", "schema_name", "region"])
    ]

if __name__ == "__main__":
    load_dotenv()
    
    parser = argparse.ArgumentParser(description='Generate coupon analytics reports')
    parser.add_argument('--schema', type=str, help='Specific schema to analyze')
    parser.add_argument('--excel', type=str, help='Output Excel file path')
    parser.add_argument('--summary', action='store_true', help='Print summary to console')
    parser.add_argument('--slack', action='store_true', help='Send report to Slack')
    args = parser.parse_args()
    
    configs = get_event_configs()
    if not configs:
        raise ValueError("No valid event configurations found in environment")
    
    # Process schemas
    schemas_to_process = [args.schema] if args.schema else [config['schema'] for config in configs]
    
    for schema in schemas_to_process:
        try:
            # Find the config for this schema
            config = next((c for c in configs if c['schema'] == schema), None)
            region = config.get('region') if config else None
            
            logger.info(f"Generating coupon analytics for schema: {schema} (region: {region})")
            
            analytics = CouponAnalytics(schema, region if region else None)
            
            if args.summary:
                analytics.print_summary()
            
            excel_path = None
            if args.excel or not args.summary:  # Generate Excel if specified or if no summary requested
                excel_path = analytics.generate_excel_report(args.excel)
            
            # Send to Slack if requested
            if args.slack and excel_path:
                analytics.send_excel_to_slack(excel_path)
            elif args.slack:
                analytics.send_slack_report()
            
            analytics.close()
            
        except Exception as e:
            logger.error(f"Failed to process schema {schema}: {e}")
            continue 