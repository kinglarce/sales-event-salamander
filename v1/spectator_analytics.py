import os
import sys
import json
import logging
from pathlib import Path

# Add project root to Python path for shared components
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models.database import Base
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv
from typing import Dict, List, Optional, Any, Tuple
import pytz

from utils.event_config import is_only_spectator_report

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
    log_filename = f'logs/spectator_analytics_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
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
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
        # Set schema for metadata
        Base.metadata.schema = schema
        self._ensure_ticket_summary_reserved_count()

    def _ensure_ticket_summary_reserved_count(self) -> None:
        """Add reserved_count if missing (manual spectator runs skip ingest setup_schema)."""
        try:
            self.session.execute(
                text(
                    f"ALTER TABLE {self.schema}.ticket_summary "
                    "ADD COLUMN IF NOT EXISTS reserved_count INTEGER DEFAULT 0"
                )
            )
            self.session.commit()
        except Exception as e:
            logger.warning(
                "Could not ensure ticket_summary.reserved_count on schema %s: %s",
                self.schema,
                e,
            )
            self.session.rollback()

    def execute_query(self, query: str, params: Dict = None) -> List:
        """Execute a SQL query with parameters"""
        try:
            # Ensure query is a string and wrap it in text()
            query_text = text(query) if isinstance(query, str) else query
            result = self.session.execute(query_text, params or {})
            return result.fetchall()
        except Exception as e:
            logger.error(f"Database query error: {e}")
            try:
                self.session.rollback()
            except Exception:
                pass
            return []
    
    def close(self):
        """Close the database session"""
        self.session.close()

class SpectatorDataProvider:
    """Provides spectator data from the database"""
    
    def __init__(self, db_manager: DatabaseManager, event_id: str, region: str):
        self.db = db_manager
        self.schema = db_manager.schema
        self.event_id = event_id
        self.region = region
        
    def get_spectator_breakdown(self) -> List[Dict[str, Any]]:
        """Get spectator breakdown using the spectator-only SQL query"""
        try:
            # Read the SQL file for spectator-only breakdown
            with open('sql/get_spectator_only_report.sql', 'r') as file:
                sql = file.read().format(SCHEMA=self.schema)

            # Execute query
            results = self.db.execute_query(sql)

            # display_category, total_sold, total_buying (RESERVED = in checkout)
            return [
                {
                    "ticket_group": row[0],
                    "sold": int(row[1] or 0),
                    "buying": int(row[2] or 0),
                }
                for row in results
            ]

        except Exception as e:
            logger.error(f"Error getting spectator breakdown: {e}")
            return []
    
    def get_spectator_totals(self) -> Tuple[int, int]:
        """Totals across all spectator rows: (sold VALID/DETAILSREQUIRED, buying RESERVED)."""
        try:
            query = f"""
                SELECT
                    COALESCE(SUM(total_count), 0),
                    COALESCE(SUM(reserved_count), 0)
                FROM {self.schema}.ticket_summary
                WHERE ticket_category = 'spectator'
            """
            results = self.db.execute_query(query)
            if results:
                return int(results[0][0] or 0), int(results[0][1] or 0)
            return 0, 0
        except Exception as e:
            logger.error(f"Error getting spectator totals: {e}")
            return 0, 0

class SlackReporter:
    """Handles reporting to Slack"""
    
    def __init__(self, schema: str, region: str):
        load_dotenv()
        self.schema = schema
        self.db_manager = DatabaseManager(schema)
        self.slack_token = os.getenv("SLACK_API_TOKEN")
        self.REGISTRATION_CHANNEL = os.getenv(
            f"EVENT_CONFIGS__{region}__REGISTRATION_CHANNEL",
            os.getenv("REGISTRATION_CHANNEL", "events-sales-tracker")
        )
        
        # Define a mapping of regions to icons
        self.icon_mapping = self.load_icon_mapping()

        # Get the icon based on the schema (which is the region)
        self.icon = self.icon_mapping.get(region, self.icon_mapping["default"])
        
        if self.slack_token:
            self.slack_client = WebClient(token=self.slack_token)
            logger.info(f"Slack client initialized with token: {self.slack_token[:5]}...")
            logger.info(f"Using Slack channel: {self.REGISTRATION_CHANNEL}")
        else:
            self.slack_client = None
            logger.warning("Slack token not found. Slack notifications will be disabled.")
            
    @staticmethod
    def load_icon_mapping():
        try:
            with open("icons.json", "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"default": "🎟️"}
    
    def format_spectator_table(self, spectator_data: List[Dict[str, Any]]) -> str:
        """Format spectator data into a Slack-friendly table"""
        if not spectator_data:
            return ""

        w_group, w_buy, w_sold = 34, 14, 12
        table = "```\n"
        hdr = (
            f"{'Ticket Group':<{w_group}} | "
            f"{'No. Pax Buying':>{w_buy}} | "
            f"{'No. Pax Sold':>{w_sold}}\n"
        )
        table += hdr
        table += f"{'-' * w_group} | {'-' * w_buy} | {'-' * w_sold}\n"

        for item in spectator_data:
            g = (item.get("ticket_group") or "")[: w_group]
            b = int(item.get("buying") or 0)
            s = int(item.get("sold") or 0)
            table += f"{g:<{w_group}} | {b:>{w_buy}} | {s:>{w_sold}}\n"

        table += "```"
        return table
    
    def send_spectator_report(
        self,
        spectator_breakdown: List[Dict[str, Any]],
        total_sold: int,
        total_buying: int,
    ) -> bool:
        """Send a spectator-only report to Slack"""
        if not self.slack_client:
            logger.warning("Slack client not available. Report not sent.")
            return False
            
        try:
            # Create message blocks with Hong Kong timezone
            hk_tz = pytz.timezone('Asia/Hong_Kong')
            current_time_hk = datetime.now(pytz.UTC).astimezone(hk_tz)

            blocks = [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{self.icon} {self.schema.upper()} Spectator Report",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"*Spectator Sales Summary:*\n"
                            f"_Updated: {current_time_hk.strftime('%Y-%m-%d %H:%M:%S')} HKT_\n"
                            f"*No. Pax Sold (confirmed):* {total_sold:,}\n"
                            f"*No. Pax Buying (reserved):* {total_buying:,}"
                        )
                    }
                }
            ]
            
            # Add spectator breakdown table
            if spectator_breakdown:
                blocks.append({"type": "divider"})
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*Spectator Breakdown by Day:*"
                    }
                })
                
                # Format spectator table
                spectator_table = self.format_spectator_table(spectator_breakdown)
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": spectator_table
                    }
                })
            else:
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*No spectator data available*"
                    }
                })
            
            # Send the message
            logger.info(f"Sending Slack message to channel: {self.REGISTRATION_CHANNEL}")
            
            response = self.slack_client.chat_postMessage(
                channel=self.REGISTRATION_CHANNEL,
                blocks=blocks
            )
            
            logger.info(f"Slack spectator report sent successfully to {self.REGISTRATION_CHANNEL}")
            return True
            
        except SlackApiError as e:
            logger.error(f"Error sending Slack report: {e.response['error']}")
            return False

class SpectatorAnalytics:
    """Main class that orchestrates the spectator analytics process"""
    
    def __init__(self, schema: str, event_id: str, region: str):
        self.schema = schema
        self.event_id = event_id
        
        # Initialize components
        self.db_manager = DatabaseManager(schema)
        self.data_provider = SpectatorDataProvider(self.db_manager, event_id, region)
        
        # Load Slack settings
        self.reporter = SlackReporter(schema, region)
    
    def run_analysis(self):
        """Run the complete spectator analysis workflow"""
        try:
            # Get spectator breakdown
            spectator_breakdown = self.data_provider.get_spectator_breakdown()
            logger.info(f"Spectator breakdown: {len(spectator_breakdown)} entries")
            
            total_sold, total_buying = self.data_provider.get_spectator_totals()
            logger.info(
                "Spectator totals — sold: %s, buying (reserved): %s",
                total_sold,
                total_buying,
            )

            success = self.reporter.send_spectator_report(
                spectator_breakdown,
                total_sold,
                total_buying,
            )
            logger.info(f"Slack report sent: {success}")
            
            logger.info(f"Spectator analysis completed successfully for event {self.event_id}")
            
        except Exception as e:
            logger.error(f"Error running spectator analysis: {e}", exc_info=True)
        finally:
            self.db_manager.close()

def main():
    load_dotenv()
    
    # Get event configurations
    configs = []
    for key, value in os.environ.items():
        if key.startswith("EVENT_CONFIGS__") and key.endswith("__schema_name"):
            region = key.split("__")[1]
            schema = value
            event_id = os.getenv(f"EVENT_CONFIGS__{region}__event_id")
            
            if event_id and is_only_spectator_report(region):
                configs.append({"schema": schema, "event_id": event_id, "region": region})
    
    if not configs:
        logger.error("No valid spectator-only event configurations found")
        return
    
    # Run analysis for each event
    for config in configs:
        logger.info(f"Running spectator analysis for schema: {config['schema']}, event_id: {config['event_id']}")
        analyzer = SpectatorAnalytics(
            config['schema'], 
            config['event_id'],
            config['region']
        )
        analyzer.run_analysis()

if __name__ == "__main__":
    main() 