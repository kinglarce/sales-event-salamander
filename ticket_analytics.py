import os
import json
import logging
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models.database import Base
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv
from typing import Dict, List, Optional, Any, Tuple
import pytz

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/ticket_analytics_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
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
    
    def execute_query(self, query: str, params: Dict = None) -> List:
        """Execute a SQL query with parameters"""
        try:
            # Ensure query is a string and wrap it in text()
            query_text = text(query) if isinstance(query, str) else query
            result = self.session.execute(query_text, params or {})
            return result.fetchall()
        except Exception as e:
            logger.error(f"Database query error: {e}")
            return []
    
    def check_table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the schema"""
        query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = '{self.schema}' 
                AND table_name = '{table_name}'
            )
        """
        result = self.execute_query(query)
        return result[0][0] if result else False
    
    def close(self):
        """Close the database session"""
        self.session.close()

class TicketDataProvider:
    """Provides ticket data from the database"""
    
    def __init__(self, db_manager: DatabaseManager, event_id: str, region: str):
        self.db = db_manager
        self.schema = db_manager.schema
        self.event_id = event_id
        self.region = region
        
    def get_capacity_configs(self) -> Dict[str, str]:
        """Get event capacity configurations"""
        try:
            with open('sql/get_event_capacity_configs.sql', 'r') as file:
                sql = file.read().format(SCHEMA=self.schema)
            result = self.db.execute_query(sql)
            return {row[0]: row[1] for row in result}
        except Exception as e:
            logger.error(f"Error getting capacity configs: {e}")
            return {}
    
    def get_current_summary(self, capacity_configs: Dict[str, str]) -> Dict[str, int]:
        """Get current summary report data"""
        try:
            # Read the SQL file
            with open('sql/get_current_summary.sql', 'r') as file:
                sql = file.read().format(SCHEMA=self.schema)
            results = self.db.execute_query(sql, {"event_id": self.event_id})
            summary = {row[0]: row[1] for row in results}
            summary['Total_excluding_spectators'] = f"{summary['Total_excluding_spectators']} / {capacity_configs.get('price_trigger', 0)}"
            return summary
            
        except Exception as e:
            logger.error(f"Error getting current summary: {e}")
            return {}
    
    def get_historical_data(self, minutes: int = 15) -> pd.DataFrame:
        """Get historical summary report data from the last N minutes"""
        time_threshold = datetime.now() - timedelta(minutes=minutes)
        
        try:
            query = f"""
                SELECT 
                    created_at as date,
                    ticket_group,
                    total_count
                FROM {self.schema}.summary_report
                WHERE 
                    event_id = :event_id 
                    AND created_at >= :time_threshold
                ORDER BY created_at ASC
            """
            
            results = self.db.execute_query(
                query, 
                {"event_id": self.event_id, "time_threshold": time_threshold}
            )
            
            # Convert to DataFrame
            df = pd.DataFrame(results, columns=['date', 'ticket_group', 'total_count'])
            
            if not df.empty:
                # Pivot the data to get ticket groups as columns
                pivot_df = df.pivot_table(
                    index='date',
                    columns='ticket_group',
                    values='total_count',
                    aggfunc='last'
                ).reset_index()
                
                return pivot_df
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Error getting historical data: {e}")
            return pd.DataFrame()
    
    def get_detailed_breakdown(self) -> List[Dict[str, Any]]:
        """Get detailed breakdown using the custom SQL query"""
        try:
            # Determine which SQL file to use
            sql_summary_detailed_by_day = 'sql/get_detailed_summary_with_day_report.sql'
            sql_summary = 'sql/get_detailed_summary_report.sql'
            is_config_breakdown_exist =  os.getenv(f'EVENT_CONFIGS__{self.region}__summary_breakdown_day', 'false').strip().lower() in ('true', '1')
            sql_file = sql_summary_detailed_by_day if is_config_breakdown_exist else sql_summary
            
            # Read and format SQL file
            with open(sql_file, 'r') as file:
                sql = file.read().format(SCHEMA=self.schema)

            # Execute query
            results = self.db.execute_query(sql)

            # Process results into a list of dictionaries
            return [{"ticket_group": row[0], "total_count": row[1]} for row in results]

        except Exception as e:
            logger.error(f"Error getting detailed breakdown: {e}")
        return []

class DataAnalyzer:
    """Handles data analysis and projections"""
    
    def calculate_growth(self, df: pd.DataFrame) -> Optional[Dict]:
        """Calculate growth metrics from historical data"""
        if df.empty or len(df) < 2:
            return None
        
        # Create a copy to avoid modifying the original
        data = df.copy()
        
        # Make sure date is the index
        if 'date' in data.columns:
            data = data.set_index('date')
        
        # Get first and last values for each column
        first_values = data.iloc[0]
        last_values = data.iloc[-1]
        
        # Calculate changes
        changes = {}
        for column in data.columns:
            # Skip non-numeric columns
            if not pd.api.types.is_numeric_dtype(data[column]):
                continue
                
            first_val = first_values[column]
            last_val = last_values[column]
            
            if pd.notna(first_val) and pd.notna(last_val) and first_val > 0:
                abs_change = last_val - first_val
                pct_change = (abs_change / first_val * 100)
                
                changes[column] = {
                    'first_value': first_val,
                    'last_value': last_val,
                    'absolute_change': abs_change,
                    'percent_change': pct_change
                }
        
        return {'changes': changes}
    
    def project_future_sales(self, df: pd.DataFrame, projection_minutes: int = 3) -> Optional[pd.DataFrame]:
        """Project future ticket sales based on historical data for a specified number of minutes"""
        if df.empty or len(df) < 2:
            logger.warning("Not enough historical data for projections")
            return None
        
        # Create a copy of the dataframe
        data = df.copy()
        
        # Make sure date is the index
        if 'date' in data.columns:
            data = data.set_index('date')
        
        # Calculate growth rates per minute for each ticket group
        growth_rates = {}
        for column in data.columns:
            # Skip non-numeric columns
            if not pd.api.types.is_numeric_dtype(data[column]):
                continue
                
            # Calculate time difference in minutes
            if isinstance(data.index[0], datetime) and isinstance(data.index[-1], datetime):
                time_diff = (data.index[-1] - data.index[0]).total_seconds() / 60
            else:
                # Default to 1 day if dates aren't datetime objects
                time_diff = 24 * 60
            
            # Use exponential growth model if we have enough data points
            if len(data) >= 3 and time_diff > 0:
                start_value = data[column].iloc[0]
                end_value = data[column].iloc[-1]
                
                if start_value > 0:
                    # Calculate compound growth rate per minute
                    minute_rate = (end_value / start_value) ** (1/time_diff) - 1
                    growth_rates[column] = minute_rate
            else:
                # Simple average of percentage changes
                pct_changes = data[column].pct_change().dropna()
                if not pct_changes.empty:
                    avg_change = pct_changes.mean()
                    growth_rates[column] = avg_change
        
        # Create projection dataframe
        last_date = data.index[-1] if isinstance(data.index[-1], datetime) else datetime.now()
        projection_dates = [last_date + timedelta(minutes=i+1) for i in range(projection_minutes)]
        projections = pd.DataFrame(index=projection_dates)
        projections.index.name = 'date'
        
        # Add date as a column for easier plotting
        projections['date'] = projections.index
        
        # Project each ticket group
        for column, rate in growth_rates.items():
            last_value = data[column].iloc[-1]
            projected_values = []
            
            for i in range(projection_minutes):
                projected_value = last_value * (1 + rate) ** (i+1)
                projected_values.append(projected_value)
            
            projections[column] = projected_values
            
            # Log the projected growth
            final_projected = projected_values[-1]
            percent_increase = ((final_projected / last_value) - 1) * 100
            logger.info(f"Projected {column} in {projection_minutes} minutes: {last_value:.0f} → {final_projected:.0f} (+{percent_increase:.1f}%)")
        
        return projections

class SlackReporter:
    """Handles reporting to Slack"""
    
    def __init__(self, schema: str, region: str):
        load_dotenv()
        self.schema = schema
        self.db_manager = DatabaseManager(schema)
        self.slack_token = os.getenv("SLACK_API_TOKEN")
        self.slack_channel = os.getenv(
            f"EVENT_CONFIGS__{region}__SLACK_CHANNEL",
            os.getenv("SLACK_CHANNEL", "events-sales-tracker")
        )
        
        # Define a mapping of regions to icons
        self.icon_mapping = self.load_icon_mapping()

        # Get the icon based on the schema (which is the region)
        self.icon = self.icon_mapping.get(region, self.icon_mapping["default"])
        
        if self.slack_token:
            self.slack_client = WebClient(token=self.slack_token)
            logger.info(f"Slack client initialized with token: {self.slack_token[:5]}...")
            logger.info(f"Using Slack channel: {self.slack_channel}")
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
    
    def format_table(self, data: List[Tuple[str, int]], title: str = "") -> str:
        """Format data into a Slack-friendly table"""
        if not data:
            return ""
        
        # Find maximum widths
        group_width = max(len(str(row[0])) for row in data)
        group_width = max(group_width, len("Ticket Group"))
        count_width = max(len(str(row[1])) for row in data)
        count_width = max(count_width, len("No. Pax"))
        
        # Create table
        table = f"{title}\n" if title else ""
        table += "```\n"
        table += f"{'Ticket Group':<{group_width}} | {'No. Pax':>10}\n"
        table += f"{'-' * group_width}-|-{'-' * 10}\n"
        
        for group, count in data:
            # Format group name (replace underscores with spaces and capitalize)
            formatted_group = ' '.join(word.capitalize() for word in group.split('_'))
            table += f"{formatted_group:<{group_width}} | {count:>10}\n"
        
        table += "```"
        return table
    
    def get_adaptive_summary(self) -> List[Tuple[str, int]]:
        """Get adaptive ticket summary"""
        try:
            with open('sql/get_adaptive_summary.sql', 'r') as file:
                sql = file.read().format(SCHEMA=self.schema)
            result = self.db_manager.execute_query(sql)
            return [(row[0], row[1]) for row in result]
        except Exception as e:
            logger.error(f"Error getting adaptive summary: {e}")
            return []
    
    def send_report(self, 
                   current_summary: Dict[str, int], 
                   detailed_breakdown: List[Dict[str, Any]],
                   capacity_configs: Dict[str, str],
                   growth_data: Optional[Dict] = None, 
                   projections: Optional[pd.DataFrame] = None,
                   projection_minutes: int = 3) -> bool:
        """Send a report to Slack with current summary and projections"""
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
                        "text": f"{self.icon} {self.schema.upper()} Sales Report",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"*Current Sales Summary:*\n"
                            f"_Updated: {current_time_hk.strftime('%Y-%m-%d %H:%M:%S')} HKT_\n"
                            f"*Price Tier:* {capacity_configs.get('price_tier', 'N/A')}\n"
                            f"_(Max Capacity: {capacity_configs.get('max_capacity', 'N/A')}, "
                            f"Start Wave: {capacity_configs.get('start_wave', 'N/A')})_"
                        )
                    }
                }
            ]
            
            # Format main summary table
            summary_data = [(k, v) for k, v in current_summary.items()]
            summary_table = self.format_table(summary_data)
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": summary_table
                }
            })
            
            # Add adaptive summary if available
            adaptive_data = self.get_adaptive_summary()
            if adaptive_data:
                adaptive_table = self.format_table(adaptive_data, "\n*Adaptive Group*")
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": adaptive_table
                    }
                })
            
            # Add detailed breakdown if available
            if detailed_breakdown:
                blocks.append({"type": "divider"})
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*Detailed Ticket Breakdown:*"
                    }
                })
                
                # Create a formatted table
                table_text = "```\n"
                table_text += f"{'Ticket Group':<40} | {'No. Pax':>10}\n"
                table_text += f"{'-'*40} | {'-'*10}\n"
                
                for item in detailed_breakdown:
                    table_text += f"{item['ticket_group']:<40} | {item['total_count']:>10}\n"
                
                table_text += "```"
                
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": table_text
                    }
                })
            
            # Add growth data if enabled and available
            if os.getenv('ENABLE_GROWTH_ANALYSIS', 'false').lower() == 'true' and growth_data and growth_data.get('changes'):
                blocks.append({"type": "divider"})
                
                # Format the time period
                period = growth_data.get('period', {})
                period_text = ""
                if period.get('days'):
                    period_text += f"{period['days']} days "
                if period.get('hours'):
                    period_text += f"{period['hours']} hours "
                if period.get('minutes'):
                    period_text += f"{period['minutes']} minutes"
                
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Growth Analysis (Last {period_text}):*"
                    }
                })
                
                # Create a formatted table for growth
                table_text = "```\n"
                table_text += f"{'Ticket Group':<30} | {'Start':>8} | {'Current':>8} | {'Change':>8} | {'Growth %':>8}\n"
                table_text += f"{'-'*30} | {'-'*8} | {'-'*8} | {'-'*8} | {'-'*8}\n"
                
                for group, data in growth_data['changes'].items():
                    table_text += f"{group:<30} | {data['first_value']:>8.0f} | {data['last_value']:>8.0f} | {data['absolute_change']:>8.0f} | {data['percent_change']:>7.1f}%\n"
                
                table_text += "```"
                
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": table_text
                    }
                })
            
            # Add projections if enabled and available
            if os.getenv('ENABLE_PROJECTIONS', 'false').lower() == 'true' and projections is not None and not projections.empty:
                blocks.append({"type": "divider"})
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{projection_minutes}-Minute Projections:*"
                    }
                })
                
                # Create a formatted table for projections
                table_text = "```\n"
                table_text += f"{'Ticket Group':<30} | {'Current':>8} | {'Projected':>10} | {'Increase':>10} | {'Growth %':>8}\n"
                table_text += f"{'-'*30} | {'-'*8} | {'-'*10} | {'-'*10} | {'-'*8}\n"
                
                # Get the last row of projections
                end_projection = projections.iloc[-1]
                
                for group in end_projection.index:
                    if group == 'date':
                        continue
                        
                    projected_count = end_projection[group]
                    current_count = current_summary.get(group, 0)
                    
                    if not pd.isna(projected_count) and current_count > 0:
                        increase = projected_count - current_count
                        percent = (increase / current_count) * 100
                        
                        table_text += f"{group:<30} | {current_count:>8.0f} | {projected_count:>10.0f} | {increase:>10.0f} | {percent:>7.1f}%\n"
                
                table_text += "```"
                
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": table_text
                    }
                })
                
                # Add a note about the projection methodology
                blocks.append({
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"_Projections are based on data from the last {projection_minutes} minutes and may vary based on actual sales patterns._"
                        }
                    ]
                })
            
            # Send the message
            logger.info(f"Sending Slack message to channel: {self.slack_channel}")
            
            response = self.slack_client.chat_postMessage(
                channel=self.slack_channel,
                blocks=blocks
            )
            
            logger.info(f"Slack report sent successfully to {self.slack_channel}")
            return True
            
        except SlackApiError as e:
            logger.error(f"Error sending Slack report: {e.response['error']}")
            return False

class TicketAnalytics:
    """Main class that orchestrates the ticket analytics process"""
    
    def __init__(self, schema: str, event_id: str, region: str,projection_minutes: int = 3, history_minutes: int = 15):
        self.schema = schema
        self.event_id = event_id
        self.projection_minutes = projection_minutes
        self.history_minutes = history_minutes
        
        # Initialize components
        self.db_manager = DatabaseManager(schema)
        self.data_provider = TicketDataProvider(self.db_manager, event_id, region)
        self.analyzer = DataAnalyzer()
        
        # Load Slack settings
        self.reporter = SlackReporter(schema, region)
    
    def run_analysis(self):
        """Run the complete analysis workflow"""
        try:
            capacity_configs = self.data_provider.get_capacity_configs()
            # Get current summary
            current_summary = self.data_provider.get_current_summary(capacity_configs)
            logger.info(f"Current summary: {current_summary}")
            
            # Get historical data
            historical_df = self.data_provider.get_historical_data(minutes=self.history_minutes)
            logger.info(f"Historical data shape: {historical_df.shape if not historical_df.empty else 'Empty'}")
            
            # Calculate growth
            growth_data = None
            if not historical_df.empty and len(historical_df) >= 2:
                growth_data = self.analyzer.calculate_growth(historical_df)
                logger.info(f"Growth data calculated: {growth_data is not None}")
            
            # Project future sales
            projection_df = None
            if not historical_df.empty and len(historical_df) >= 2:
                projection_df = self.analyzer.project_future_sales(historical_df, self.projection_minutes)
                logger.info(f"Projection data calculated for {self.projection_minutes} minutes")
            
            # Send to Slack
            success = self.reporter.send_report(
                current_summary, 
                self.data_provider.get_detailed_breakdown(),
                capacity_configs,
                growth_data, 
                projection_df,
                self.projection_minutes
            )
            logger.info(f"Slack report sent: {success}")
            
            logger.info(f"Analysis completed successfully for event {self.event_id}")
            
        except Exception as e:
            logger.error(f"Error running analysis: {e}", exc_info=True)
        finally:
            self.db_manager.close()

def main():
    load_dotenv()
    
    # Get projection minutes from environment or use default
    projection_minutes = int(os.getenv("PROJECTION_MINUTES", "3"))
    history_minutes = int(os.getenv("HISTORY_MINUTES", "15"))
    
    # Get event configurations
    configs = []
    for key, value in os.environ.items():
        if key.startswith("EVENT_CONFIGS__") and key.endswith("__schema_name"):
            region = key.split("__")[1]
            schema = value
            event_id = os.getenv(f"EVENT_CONFIGS__{region}__event_id")
            if event_id:
                configs.append({"schema": schema, "event_id": event_id, "region": region})
    
    if not configs:
        logger.error("No valid event configurations found")
        return
    
    # Run analysis for each event
    for config in configs:
        logger.info(f"Running analysis for schema: {config['schema']}, event_id: {config['event_id']}")
        analyzer = TicketAnalytics(
            config['schema'], 
            config['event_id'],
            config['region'],
            projection_minutes=projection_minutes,
            history_minutes=history_minutes
        )
        analyzer.run_analysis()

if __name__ == "__main__":
    main() 