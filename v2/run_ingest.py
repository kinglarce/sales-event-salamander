#!/usr/bin/env python3
"""
V2 Ingest Runner
A clean v2 system that uses the default pipeline configuration.
"""

import sys
import os
import yaml
import logging
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def setup_logging():
    """Setup logging for v2 system"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'logs/v2_ingest_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        ]
    )

def load_pipeline_config(config_path: str = None) -> Dict[str, Any]:
    """Load pipeline configuration from YAML file"""
    if config_path is None:
        config_path = "v2/pipeline_configs/default.yaml"
    
    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except Exception as e:
        logging.error(f"Failed to load pipeline config from {config_path}: {e}")
        raise

def run_script(script_name: str, timeout: float = 300.0, skip_fetch: bool = False) -> bool:
    """Run a Python script and return True if successful"""
    logger = logging.getLogger(__name__)
    try:
        logger.info(f"🚀 Starting {script_name}...")
        
        # Build command with arguments
        cmd = [sys.executable, script_name]
        if skip_fetch:
            cmd.append('--skip-fetch')
        
        # Run the script
        result = subprocess.run(
            cmd, 
            cwd=project_root,
            timeout=timeout,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            logger.info(f"✅ {script_name} completed successfully")
            if result.stdout:
                logger.debug(f"{script_name} output: {result.stdout}")
            return True
        else:
            logger.error(f"❌ {script_name} failed with return code {result.returncode}")
            logger.error(f"Error output: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"⏰ {script_name} timed out after {timeout} seconds")
        return False
    except Exception as e:
        logger.error(f"💥 Unexpected error running {script_name}: {e}")
        return False

def execute_pipeline(config: Dict[str, Any], skip_fetch: bool = False) -> bool:
    """Execute the pipeline based on configuration"""
    logger = logging.getLogger(__name__)
    
    # Get pipeline steps
    steps = config.get('steps', [])
    stop_on_failure = config.get('stop_on_failure', True)
    
    logger.info(f"📋 Executing pipeline: {config.get('name', 'Unknown')}")
    logger.info(f"📝 Description: {config.get('description', 'No description')}")
    logger.info(f"🔄 Steps to execute: {len(steps)}")
    
    # Track completed steps
    completed_steps = set()
    
    for step in steps:
        step_name = step.get('name')
        function = step.get('function')
        enabled = step.get('enabled', True)
        timeout = step.get('timeout', 300.0)
        depends_on = step.get('depends_on', [])
        
        if not enabled:
            logger.info(f"⏭️  Skipping disabled step: {step_name}")
            continue
            
        # Check dependencies
        if depends_on:
            missing_deps = [dep for dep in depends_on if dep not in completed_steps]
            if missing_deps:
                logger.error(f"❌ Step {step_name} has unmet dependencies: {missing_deps}")
                if stop_on_failure:
                    return False
                continue
        
        # Map function names to actual scripts
        # V2 system uses V2 scripts with core improvements
        script_mapping = {
            'static_data_ingestion': 'v2/ingest_static_data.py',
            'events_tickets_ingestion': 'v2/ingest_events_tickets.py',
            'coupon_ingestion': 'v2/ingest_coupons.py',
            'age_groups_ingestion': 'v2/ingest_age_groups.py',
            'gender_fix_ingestion': 'v2/ingest_gender_fix.py',
            'analytics_processing': 'v1/ticket_analytics.py'  # Keep v1 for analytics
        }
        
        script_path = script_mapping.get(function)
        if not script_path:
            logger.error(f"❌ Unknown function: {function}")
            if stop_on_failure:
                return False
            continue
        
        # Run the step
        success = run_script(script_path, timeout, skip_fetch)
        
        if success:
            completed_steps.add(step_name)
            logger.info(f"✅ Step {step_name} completed successfully")
        else:
            logger.error(f"❌ Step {step_name} failed")
            if stop_on_failure:
                logger.error("🛑 Stopping pipeline due to step failure")
                return False
    
    logger.info(f"🎉 Pipeline completed successfully! Completed steps: {len(completed_steps)}")
    return True

def main():
    """Main entry point for v2 system"""
    import argparse
    
    parser = argparse.ArgumentParser(description="V2 Ingest System")
    parser.add_argument('--pipeline-config', help='Path to pipeline configuration file')
    parser.add_argument('--pipeline-name', help='Name of predefined pipeline')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--skip-fetch', action='store_true', help='Skip API calls')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        logger.info("🚀 Starting V2 Ingest System...")
        
        # Load pipeline configuration
        if args.pipeline_config:
            config = load_pipeline_config(args.pipeline_config)
            logger.info(f"📄 Using custom pipeline config: {args.pipeline_config}")
        elif args.pipeline_name:
            # Handle predefined pipelines
            predefined_pipelines = {
                'default': 'v2/pipeline_configs/default.yaml',
                'minimal': 'v2/pipeline_configs/minimal.yaml',
                'parallel': 'v2/pipeline_configs/parallel.yaml'
            }
            
            if args.pipeline_name in predefined_pipelines:
                config_path = predefined_pipelines[args.pipeline_name]
                config = load_pipeline_config(config_path)
                logger.info(f"📄 Using predefined pipeline: {args.pipeline_name}")
            else:
                logger.error(f"❌ Unknown pipeline name: {args.pipeline_name}")
                logger.info(f"Available pipelines: {list(predefined_pipelines.keys())}")
                sys.exit(1)
        else:
            # Use default pipeline
            config = load_pipeline_config()
            logger.info("📄 Using default pipeline configuration")
        
        # Execute the pipeline
        success = execute_pipeline(config, args.skip_fetch)
        
        if success:
            logger.info("🎉 V2 system completed successfully!")
            sys.exit(0)
        else:
            logger.error("💥 V2 system failed!")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"💥 V2 system error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()