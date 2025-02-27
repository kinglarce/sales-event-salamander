import os
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models.database import Base
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv
import json
from typing import Dict, List, Optional, Tuple, Any, Union

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
        self.engine = create_engine("postgresql://postgres:postgres@postgres:5432/vivenu_db")
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
        # Set schema for metadata
        Base.metadata.schema = schema
    
    def execute_query(self, query: str, params: Dict = None) -> List[Tuple]:
        """Execute a SQL query with parameters"""
        try:
            result = self.session.execute(text(query), params or {})
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

class DataAnalyzer:
    """Handles data analysis and projections"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def calculate_growth(self, df: pd.DataFrame) -> Dict:
        """Calculate growth metrics from historical data"""
        if df.empty or len(df) < 2:
            return {}
        
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
            
            if pd.notna(first_val) and pd.notna(last_val):
                abs_change = last_val - first_val
                pct_change = (abs_change / first_val * 100) if first_val > 0 else 0
                
                changes[column] = {
                    'first_value': first_val,
                    'last_value': last_val,
                    'absolute_change': abs_change,
                    'percent_change': pct_change
                }
        
        # Calculate time period
        if isinstance(data.index[0], datetime) and isinstance(data.index[-1], datetime):
            time_diff = data.index[-1] - data.index[0]
            period = {
                'days': time_diff.days,
                'hours': time_diff.seconds // 3600,
                'minutes': (time_diff.seconds % 3600) // 60
            }
        else:
            period = {'days': 0, 'hours': 0, 'minutes': 0}
        
        return {
            'changes': changes,
            'period': period
        }
    
    def project_future_sales(self, df: pd.DataFrame, projection_minutes: int = 3) -> pd.DataFrame:
        """Project future ticket sales based on historical data for a specified number of minutes"""
        if df.empty or len(df) < 2:
            logger.warning("Not enough historical data for projections")
            return pd.DataFrame()
        
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

class TicketDataProvider:
    """Provides ticket data from the database"""
    
    def __init__(self, db_manager: DatabaseManager, event_id: str):
        self.db = db_manager
        self.event_id = event_id
        self.schema = db_manager.schema
    
    def get_current_summary(self) -> Dict[str, int]:
        """Get current summary report data"""
        try:
            # First check if the table exists
            if self.db.check_table_exists('summary_report'):
                # Query with explicit schema
                query = f"""
                    SELECT ticket_group, total_count
                    FROM {self.schema}.summary_report
                    WHERE event_id = :event_id
                """
                
                results = self.db.execute_query(query, {"event_id": self.event_id})
                return {row[0]: row[1] for row in results}
            else:
                logger.warning(f"Table {self.schema}.summary_report does not exist")
                # Fall back to getting data from ticket_type_summary
                return self.get_summary_from_ticket_types()
        
        except Exception as e:
            logger.error(f"Error getting current summary: {e}")
            return {}
    
    def get_detailed_breakdown(self) -> List[Dict[str, Any]]:
        """Get detailed breakdown using the custom SQL query"""
        try:
            # Read the SQL file
            with open('sql/get_summary_report.sql', 'r') as file:
                sql_template = file.read()
            
            # Replace {SCHEMA} with the actual schema
            sql_query = sql_template.replace('{SCHEMA}', self.schema)
            
            # Execute the query
            results = self.db.execute_query(sql_query)
            
            # Return as a list of dictionaries for easier formatting
            return [{"ticket_group": row[0], "total_count": row[1]} for row in results]
            
        except Exception as e:
            logger.error(f"Error getting detailed breakdown: {e}")
            return []
    
    def get_summary_from_ticket_types(self) -> Dict[str, int]:
        """Get summary data from ticket_type_summary when summary_report doesn't exist"""
        try:
            # Query ticket type summary data
            query = f"""
                SELECT 
                    CASE 
                        WHEN group_name = 'single' THEN 'All_singles'
                        WHEN group_name = 'double' THEN 'All_doubles'
                        WHEN group_name = 'relay' THEN 'All_relays'
                        WHEN group_name = 'spectator' THEN 'Spectators'
                        ELSE group_name
                    END as ticket_group,
                    SUM(total_count) as total_count
                FROM {self.schema}.ticket_type_summary
                WHERE event_id = :event_id
                GROUP BY 
                    CASE 
                        WHEN group_name = 'single' THEN 'All_singles'
                        WHEN group_name = 'double' THEN 'All_doubles'
                        WHEN group_name = 'relay' THEN 'All_relays'
                        WHEN group_name = 'spectator' THEN 'Spectators'
                        ELSE group_name
                    END
            """
            
            results = self.db.execute_query(query, {"event_id": self.event_id})
            
            # Create a summary dictionary
            summary = {row[0]: row[1] for row in results}
            
            # Add total excluding spectators
            if 'Spectators' in summary:
                total = sum(count for group, count in summary.items() if group != 'Spectators')
                summary['Total_excluding_spectators'] = total
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting summary from ticket types: {e}")
            return {}
    
    def get_historical_data(self, minutes: int = 30) -> pd.DataFrame:
        """Get historical summary report data from the last N minutes"""
        # Calculate the date N minutes ago
        start_date = datetime.now() - timedelta(minutes=minutes)
        
        try:
            # Check if summary_report table exists
            if not self.db.check_table_exists('summary_report'):
                logger.warning(f"Table {self.schema}.summary_report does not exist for historical data")
                return pd.DataFrame()
            
            # Query historical data from summary_report with timestamps
            query = f"""
                SELECT 
                    ticket_group, 
                    total_count, 
                    updated_at as date
                FROM {self.schema}.summary_report
                WHERE event_id = :event_id
                AND updated_at >= :start_date
                ORDER BY updated_at
            """
            
            historical_data = self.db.execute_query(
                query, 
                {"event_id": self.event_id, "start_date": start_date}
            )
            
            # Convert to DataFrame for easier analysis
            df = pd.DataFrame(historical_data, columns=['ticket_group', 'total_count', 'date'])
            
            # If we have data, pivot it to get ticket groups as columns
            if not df.empty:
                # Get the latest count for each date and ticket group
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

class SlackReporter:
    """Handles reporting to Slack"""
    
    def __init__(self, channel: str, schema: str):
        load_dotenv()
        self.slack_token = os.getenv("SLACK_API_TOKEN")
        self.slack_channel = channel.replace('#', '')
        self.schema = schema
        
        if self.slack_token:
            self.slack_client = WebClient(token=self.slack_token)
            logger.info(f"Slack client initialized with token: {self.slack_token[:5]}...")
        else:
            self.slack_client = None
            logger.warning("Slack token not found. Slack notifications will be disabled.")
    
    def send_report(self, 
                   current_summary: Dict[str, int], 
                   detailed_breakdown: List[Dict[str, Any]], 
                   growth_data: Dict = None, 
                   projections: pd.DataFrame = None,
                   projection_minutes: int = 3) -> bool:
        """Send a report to Slack with current summary and projections"""
        if not self.slack_client:
            logger.warning("Slack client not available. Report not sent.")
            return False
            
        try:
            # Create message blocks
            blocks = [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"🎟️ {self.schema} Ticket Analytics Report",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Current Ticket Summary:*\n_Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_"
                    }
                }
            ]
            
            # Add current summary
            for group, count in current_summary.items():
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"• *{group}:* {count} tickets"
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
                table_text += f"{'Ticket Group':<40} | {'Count':>10}\n"
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
            
            # Add growth data if available
            if growth_data and 'changes' in growth_data:
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
            
            # Add projections if available
            if projections is not None and not projections.empty:
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
    
    def __init__(self, schema: str, event_id: str, projection_minutes: int = 3, history_minutes: int = 30):
        self.schema = schema
        self.event_id = event_id
        self.projection_minutes = projection_minutes
        self.history_minutes = history_minutes
        
        # Initialize components
        self.db_manager = DatabaseManager(schema)
        self.data_provider = TicketDataProvider(self.db_manager, event_id)
        self.analyzer = DataAnalyzer(self.db_manager)
        
        # Load Slack settings
        load_dotenv()
        slack_channel = os.getenv("SLACK_CHANNEL", "events-sentry")
        self.reporter = SlackReporter(slack_channel, schema)
    
    def run_analysis(self):
        """Run the complete analysis workflow"""
        try:
            # Get current summary
            current_summary = self.data_provider.get_current_summary()
            logger.info(f"Current summary: {current_summary}")
            
            # Get detailed breakdown
            detailed_breakdown = self.data_provider.get_detailed_breakdown()
            
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
                detailed_breakdown,
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
    history_minutes = int(os.getenv("HISTORY_MINUTES", "30"))
    
    # Get event configurations
    configs = []
    for key, value in os.environ.items():
        if key.startswith("EVENT_CONFIGS__") and key.endswith("__schema_name"):
            region = key.split("__")[1]
            schema = value
            event_id = os.getenv(f"EVENT_CONFIGS__{region}__event_id")
            if event_id:
                configs.append({"schema": schema, "event_id": event_id})
    
    if not configs:
        logger.error("No valid event configurations found")
        return
    
    # Run analysis for each event
    for config in configs:
        logger.info(f"Running analysis for schema: {config['schema']}, event_id: {config['event_id']}")
        analyzer = TicketAnalytics(
            config['schema'], 
            config['event_id'],
            projection_minutes=projection_minutes,
            history_minutes=history_minutes
        )
        analyzer.run_analysis()

if __name__ == "__main__":
    main() 