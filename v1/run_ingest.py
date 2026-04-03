import subprocess
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from utils.event_config import any_only_spectator_report

# Load environment variables
load_dotenv()

# Create logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Add the console handler to the logger
logger.addHandler(console_handler)

# Check if file logging is enabled
if os.getenv('ENABLE_FILE_LOGGING', 'true').strip().lower() in ('true', '1'):
    log_filename = f'logs/cron_{datetime.now().strftime("%Y%m%d")}.log'
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

# Get the absolute path to Python executable
PYTHON_PATH = sys.executable

def run_script(script_name: str) -> bool:
    """Run a Python script and return True if successful"""
    try:
        logger.info(f"Starting {script_name}...")
        # Pass environment variables to the subprocess
        env = os.environ.copy()
        result = subprocess.run(
            [PYTHON_PATH, script_name], 
            check=True,
            capture_output=True,
            text=True,
            env=env
        )
        logger.info(f"{script_name} completed successfully")
        if result.stdout:
            logger.debug(f"{script_name} output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"{script_name} failed with error: {e}")
        logger.error(f"Stdout: {e.stdout}")
        logger.error(f"Stderr: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error running {script_name}: {e}")
        return False

def main():
    """Main function to orchestrate the scripts"""
    try:
        # Create logs directory if it doesn't exist
        os.makedirs('logs', exist_ok=True)
        
        logger.info(f"Using Python at: {PYTHON_PATH}")

        # Phase 1: fetch + aggregate into DB (must complete before any Slack/reporting)
        ingest_scripts = [
            'v1/ingest_static_data.py',
            'v1/ingest_events_tickets.py',
            # 'v1/ingest_age_groups.py',  # manual when needed
        ]
        for script in ingest_scripts:
            if not run_script(script):
                logger.error(f"Stopping: ingest failed at {script}; skipping analytics")
                return

        # Phase 2: reporting (reads fresh data from DB)
        if not run_script('v1/ticket_analytics.py'):
            logger.error("ticket_analytics failed; continuing to spectator if configured")

        if any_only_spectator_report():
            if not run_script('v1/spectator_analytics.py'):
                logger.error("spectator_analytics failed")

    except Exception as e:
        logger.error(f"Error in main orchestration: {e}")

if __name__ == "__main__":
    main() 