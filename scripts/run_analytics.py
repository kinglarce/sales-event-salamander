import subprocess
import logging
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

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
        
        # Define the execution order
        scripts = [
            'ingest_static_data.py',  # Run static data ingest first
            'ingest.py',              # Then run main ingest
            'ticket_analytics.py'      # Finally run analytics
        ]
        
        # Run scripts in sequence, stop if any fails
        for script in scripts:
            if not run_script(script):
                logger.error(f"Stopping execution due to failure in {script}")
                break
            
    except Exception as e:
        logger.error(f"Error in main orchestration: {e}")

if __name__ == "__main__":
    main() 