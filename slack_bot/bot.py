import os
import logging
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from dotenv import load_dotenv
from .handlers import setup_handlers

# Load environment variables
load_dotenv()

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SlackBot:
    def __init__(self):
        self.app = App(
            token=os.environ.get("SLACK_BOT_TOKEN"),
            signing_secret=os.environ.get("SLACK_SIGNING_SECRET")
        )
        setup_handlers(self.app) 
        logger.info("SlackBot initialized successfully")

    def start(self):
        """Start the Slack bot using Socket Mode."""
        handler = SocketModeHandler(self.app, os.environ.get("SLACK_BOT_APP_TOKEN"))
        handler.start()
        logger.info("⚡️ Slack bot is running in Socket Mode!")

if __name__ == "__main__":
    bot = SlackBot()
    bot.start() 