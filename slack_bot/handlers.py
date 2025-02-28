from typing import Dict, List, Optional
import os
import logging
from slack_bolt import Ack, Say
from slack_bot.bot_queries import BotQueries
from models.database import Ticket
from slack_bot.message_builder import SlackMessageBuilder

logger = logging.getLogger(__name__)

def setup_handlers(app):
    """Set up all event and action handlers."""
    handler = SlackHandlers(app)
    
    # Basic handlers
    app.event("app_mention")(handler.handle_mention)
    
    # Region handlers (first step)
    app.action("region_*")(handler.handle_region_selection)
    
    # Query handlers (after region selection)
    app.action("main_menu_ticket_count")(handler.handle_ticket_count)
    app.action("main_menu_registrant_search")(handler.handle_registrant_search)
    app.action("main_menu_event_status")(handler.handle_event_status)
    
    # Search handlers
    app.action("registrant_input")(handler.handle_registrant_search_input)
    
    return handler

class SlackHandlers:
    def __init__(self, app):
        self.app = app
        self.queries: Optional[BotQueries] = None
        self.message_builder = SlackMessageBuilder()

    def set_schema(self, schema: str, event_id: str = None):
        """Set the schema and event_id for the current request"""
        if schema:
            self.queries = BotQueries(schema, event_id)
            logger.info(f"Schema set to: {schema}, Event ID: {event_id}")
        else:
            logger.error("Attempted to set schema with None value")

    def handle_mention(self, event, say):
        """Handle when the bot is mentioned - show region selection first."""
        self.show_region_selection(say)

    def show_region_selection(self, say):
        """Show available regions as the first step."""
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "Hello! 👋 Please select a region to continue:"
                }
            },
            {
                "type": "actions",
                "elements": []
            }
        ]

        # Add region buttons from environment variables
        region_buttons = []
        for key in os.environ:
            if key.startswith("EVENT_CONFIGS__") and key.endswith("__schema_name"):
                region = key.split("__")[1]
                schema = os.environ[key]
                region_buttons.append({
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": region.replace("-", " ").title()
                    },
                    "value": schema,
                    "action_id": f"region_{region}"
                })

        blocks[1]["elements"] = region_buttons
        say(blocks=blocks)

    def handle_region_selection(self, ack: Ack, body: dict, say: Say):
        """Handle region selection and show main menu options."""
        ack()
        action_id = body["actions"][0]["action_id"]
        region = action_id.replace("region_", "")
        schema = os.getenv(f"EVENT_CONFIGS__{region}__schema_name")
        event_id = os.getenv(f"EVENT_CONFIGS__{region}__event_id")

        if not schema or not event_id:
            say(f"Configuration not found for region: {region}")
            return

        # Set schema and event_id for subsequent queries
        self.set_schema(schema, event_id)
        
        # Show main menu options
        self.show_main_menu(say, schema)

    def show_main_menu(self, say, schema: str):
        """Show the main menu with options."""
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"What would you like to know about {schema.upper()}?"
                }
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": text
                        },
                        "value": schema,
                        "action_id": f"main_menu_{action_id}"
                    } for action_id, text in self.get_main_menu_options().items()
                ]
            }
        ]
        say(blocks=blocks)

    def get_main_menu_options(self):
        """Return the main menu options."""
        return {
            "ticket_count": "📊 Check ticket counts",
            "registrant_search": "🔍 Search registrant",
            "event_status": "📈 Event status",
            "sales_trend": "📊 Sales trend",
            "capacity_info": "ℹ️ Capacity info"
        }

    def handle_ticket_count(self, ack: Ack, body: dict, say: Say):
        """Handle ticket count request"""
        ack()
        schema = body["actions"][0]["value"]
        
        try:
            counts = self.queries.get_ticket_counts()
            if not counts:
                say(f"No ticket count data available for {schema.upper()}.")
                return
            
            blocks = self.message_builder.build_ticket_count_message(counts, schema)
            say(blocks=blocks)
            
        except Exception as e:
            logger.error(f"Error handling ticket count: {e}")
            say("Sorry, I encountered an error while fetching ticket counts.")

    def handle_registrant_search_input(self, ack, body, say):
        """Handle registrant search input."""
        ack()
        
        try:
            search_term = body["state"]["values"][body["actions"][0]["block_id"]]["registrant_input"]["value"]
            schema = body["actions"][0]["value"].split("_")[0]
            
            self.set_schema(schema)
            registrants = self.queries.search_registrants(search_term)
            
            if not registrants:
                say(f"No registrants found matching '{search_term}' in {schema.upper()}.")
                return
            
            blocks = self.format_registrant_blocks(registrants, search_term)
            say(blocks=blocks)
            
        except Exception as e:
            logger.error(f"Error searching registrant: {e}")
            say("Sorry, I encountered an error while searching for the registrant.")

    def format_registrant_blocks(self, registrants: List[Ticket], search_term: str) -> List[dict]:
        """Format registrant search results into Slack blocks"""
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"Search Results for '{search_term}'"
                }
            }
        ]
        
        for registrant in registrants:
            name = f"{registrant.firstname or ''} {registrant.lastname or ''}".strip() or 'N/A'
            created_at = registrant.created_at.strftime('%Y-%m-%d %H:%M:%S') if registrant.created_at else 'N/A'
            
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*Order ID:* {registrant.transaction_id or 'N/A'}\n"
                        f"*Email:* {registrant.email or 'N/A'}\n"
                        f"*Name:* {name}\n"
                        f"*Ticket Type:* {registrant.ticket_name or 'N/A'}\n"
                        f"*Status:* {registrant.status or 'N/A'}\n"
                        f"*Created:* {created_at}"
                    )
                }
            })
            blocks.append({"type": "divider"})
        
        return blocks

    def handle_region_bangkok(self, ack, body, say):
        """Handle actions for the Bangkok region."""
        ack()
        # Use the generic region selection handler
        self.handle_region_selection(ack, body, say)

    def handle_region_taipei(self, ack, body, say):
        """Handle actions for the Taipei region."""
        ack()
        # Use the generic region selection handler
        self.handle_region_selection(ack, body, say)

    def format_ticket_details(self, ticket_details, formatted_category, schema):
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"Detailed View: {formatted_category} in {schema.upper()}"
                }
            }
        ]
        
        # Group by event day
        day_groups = {}
        for detail in ticket_details:
            day = detail.ticket_event_day or "Unspecified"
            if day not in day_groups:
                day_groups[day] = []
            day_groups[day].append(detail)
        
        # Add each day as a section
        for day, details in day_groups.items():
            day_text = f"*{day}:*\n"
            total_day_count = 0
            
            for detail in details:
                day_text += f"• {detail.ticket_name}: {detail.count}\n"
                total_day_count += detail.count
            
            day_text += f"\nTotal for {day}: {total_day_count}"
            
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": day_text
                }
            })
        
        # Add total count
        total_count = sum(detail.count for detail in ticket_details)
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Total {formatted_category} Tickets:* {total_count}"
            }
        })
        
        return blocks

    def format_hourly_analysis(self, hourly_sales, schema):
        blocks = [
            f"*Hourly Sales Analysis for {schema.upper()}*\n",
            "```",
            f"{'Hour':^6} | {'Sales':^8} | {'Trend':^10}",
            "-" * 28
        ]
        
        for row in hourly_sales:
            hour = int(row.hour)
            count = row.count
            hour_str = f"{hour:02d}:00"
            blocks.append(f"{hour_str:^6} | {count:^8} | {'📈' if count > 0 else '➖'}")
        
        blocks.append("```")
        return blocks

    def handle_event_status(self, ack: Ack, body: dict, say: Say):
        """Handle event status request"""
        ack()
        action = body["actions"][0]
        schema = action["value"]
        
        try:
            self.set_schema(schema)
            self.show_event_status(body, say, schema)
        except Exception as e:
            logger.error(f"Error handling event status: {e}")
            say("Sorry, I encountered an error while fetching event status.")

    def handle_registrant_search(self, ack: Ack, body: dict, say: Say):
        """Handle registrant search request"""
        ack()
        action = body["actions"][0]
        schema = action["value"]
        
        try:
            self.set_schema(schema)
            self.ask_for_registrant_info(body, say, schema)
        except Exception as e:
            logger.error(f"Error handling registrant search: {e}")
            say("Sorry, I encountered an error while setting up registrant search.")

    def format_sales_trend_blocks(self, sales_data: Dict[str, int], schema: str) -> List[dict]:
        """Format sales trend data into Slack blocks"""
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"Sales Trend for {schema.upper()} (Last 7 Days)"
                }
            }
        ]
        
        # Create the trend table
        trend_text = "```\n"
        trend_text += f"{'Date':<12} | {'Sales':>6} | {'Trend':>10}\n"
        trend_text += f"{'-'*12}-|-{'-'*6}-|-{'-'*10}\n"
        
        prev_sales = None
        for date, sales in sorted(sales_data.items()):
            trend = ""
            if prev_sales is not None:
                if sales > prev_sales:
                    trend = "📈 ↑"
                elif sales < prev_sales:
                    trend = "📉 ↓"
                else:
                    trend = "➡️ ="
            
            trend_text += f"{date:<12} | {sales:>6} | {trend:>10}\n"
            prev_sales = sales
        
        trend_text += "```"
        
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": trend_text
            }
        })
        
        return blocks

    def show_event_status(self, body, say, schema: str):
        """Show event status including sales information."""
        try:
            # Get event information
            event = self.queries.get_event_info()
            if not event:
                say(f"No event information found for {schema.upper()}.")
                return
            
            # Get ticket counts
            ticket_counts = self.queries.get_ticket_counts()
            
            # Format the response
            blocks = [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"Event Status for {schema.upper()}"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"*Event:* {event.name or 'N/A'}\n"
                            f"*Location:* {event.location_name or 'N/A'}\n"
                            f"*Start Date:* {event.start_date.strftime('%Y-%m-%d') if event.start_date else 'N/A'}\n"
                            f"*End Date:* {event.end_date.strftime('%Y-%m-%d') if event.end_date else 'N/A'}\n"
                            f"*Timezone:* {event.timezone or 'N/A'}"
                        )
                    }
                }
            ]
            
            # Add ticket counts
            ticket_text = "*Current Sales:*\n"
            total_count = 0
            
            for category, count in ticket_counts.items():
                formatted_category = " ".join(word.capitalize() for word in category.split("_"))
                ticket_text += f"{formatted_category}: {count}\n"
                total_count += count
            
            ticket_text += f"\n*Total Tickets Sold:* {total_count}"
            
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": ticket_text
                }
            })
            
            # Add action buttons for more detailed views
            blocks.append({
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "View Sales Trend"
                        },
                        "value": f"{schema}_sales_trend",
                        "action_id": "main_menu_sales_trend"
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "View Ticket Counts"
                        },
                        "value": f"{schema}_ticket_count",
                        "action_id": "main_menu_ticket_count"
                    }
                ]
            })
            
            say(blocks=blocks)
            
        except Exception as e:
            logger.error(f"Error getting event status: {e}")
            say("Sorry, I encountered an error while fetching the event status.") 