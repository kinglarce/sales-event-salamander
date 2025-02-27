import subprocess
import logging
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/cron_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Get the absolute path to Python executable
PYTHON_PATH = sys.executable

def run_ingest():
    """Run the ingest script and return True if successful"""
    try:
        logger.info("Starting ingest process...")
        # Pass environment variables to the subprocess
        env = os.environ.copy()
        result = subprocess.run(
            [PYTHON_PATH, 'ingest.py'], 
            check=True,
            capture_output=True,
            text=True,
            env=env
        )
        logger.info("Ingest completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Ingest failed with error: {e}")
        logger.error(f"Stdout: {e.stdout}")
        logger.error(f"Stderr: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error running ingest: {e}")
        return False

def run_analytics():
    """Run the ticket analytics script"""
    try:
        logger.info("Starting ticket analytics...")
        result = subprocess.run(
            [PYTHON_PATH, 'ticket_analytics.py'],
            check=True,
            capture_output=True,
            text=True
        )
        logger.info("Ticket analytics completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Ticket analytics failed with error: {e}")
        logger.error(f"Stdout: {e.stdout}")
        logger.error(f"Stderr: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error running analytics: {e}")
        return False

def main():
    """Main function to orchestrate the scripts"""
    try:
        # Create logs directory if it doesn't exist
        os.makedirs('logs', exist_ok=True)
        
        logger.info(f"Using Python at: {PYTHON_PATH}")
        
        # Run ingest first
        if run_ingest():
            # Only run analytics if ingest was successful
            run_analytics()
        else:
            logger.error("Skipping analytics due to ingest failure")
            
    except Exception as e:
        logger.error(f"Error in main orchestration: {e}")

if __name__ == "__main__":
    main() 