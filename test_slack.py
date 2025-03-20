import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_slack_connection():
    """Test Slack API connection and permissions"""
    load_dotenv()
    
    slack_token = os.getenv("SLACK_API_TOKEN")
    REGISTRATION_CHANNEL = os.getenv("REGISTRATION_CHANNEL", "")
    
    if not slack_token:
        logger.error("Slack token not found in environment variables")
        return False
    
    client = WebClient(token=slack_token)
    
    try:
        # Test basic message posting
        response = client.chat_postMessage(
            channel=REGISTRATION_CHANNEL,
            text="🤖 zZzZzZ"
        )
        logger.info(f"Message sent successfully: {response['ts']}")
        
        return True
        
    except SlackApiError as e:
        logger.error(f"Error testing Slack connection: {e.response['error']}")
        logger.error(f"Required scopes: chat:write, files:write")
        return False

if __name__ == "__main__":
    success = test_slack_connection()
    print(f"Slack connection test {'succeeded' if success else 'failed'}")