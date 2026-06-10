#!/usr/bin/env python3
"""
Vivenu Events Ticket Scrapper - Main Entry Point
Supports both v1 and v2 systems with automatic detection.
"""

import sys
import os
import argparse
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def main():
    """Main entry point with version selection"""
    parser = argparse.ArgumentParser(
        description="Vivenu Events Ticket Scrapper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run v1 system (default)
  python main.py
  
  # Run v2 system
  python main.py --version v2
  
  # Run v2 with custom pipeline
  python main.py --version v2 --pipeline-config v2/pipeline_configs/custom.yaml
  
  # Run v1 with specific script
  python main.py --version v1 --script ingest_events_tickets
  
  # Docker usage (runs DNS + TCP check to EVENT_API_BASE_URL first)
  docker exec -it vivenu-app python main.py --version v1
  docker exec -it vivenu-app python main.py --version v2

  # Skip connectivity probe (offline / known-good network)
  python main.py --version v1 --skip-connectivity-check
        """
    )
    
    # Check for environment variables first (for Docker usage)
    version = os.getenv('VERSION', 'v1')
    debug = os.getenv('DEBUG_MODE', 'false').lower() in ('true', '1')
    skip_fetch = os.getenv('SKIP_FETCH', 'false').lower() in ('true', '1')
    pipeline_config = os.getenv('PIPELINE_CONFIG')
    pipeline_name = os.getenv('PIPELINE_NAME')
    
    parser.add_argument(
        '--version', 
        choices=['v1', 'v2'], 
        default=version,
        help='Version to run (default: v1, can be set with VERSION env var)'
    )
    
    parser.add_argument(
        '--script',
        help='Specific script to run (v1 only)'
    )
    
    # Add v2 specific arguments
    parser.add_argument('--debug', action='store_true', default=debug, help='Enable debug logging')
    parser.add_argument('--skip-fetch', action='store_true', default=skip_fetch, help='Skip API calls and only update summaries')
    parser.add_argument('--pipeline-config', default=pipeline_config, help='Path to pipeline configuration file (v2 only)')
    parser.add_argument('--pipeline-name', default=pipeline_name, help='Name of predefined pipeline to use (v2 only)')
    parser.add_argument(
        '--skip-connectivity-check',
        action='store_true',
        help='Skip DNS/TCP probe for EVENT_API_BASE_URL (or set SKIP_CONNECTIVITY_CHECK=true)',
    )

    args = parser.parse_args()
    if os.getenv('SKIP_CONNECTIVITY_CHECK', '').lower() in ('true', '1'):
        args.skip_connectivity_check = True
    
    if args.version == 'v1':
        run_v1(args)
    elif args.version == 'v2':
        run_v2(args)

def run_v1(args):
    """Run v1 system"""
    print("🚀 Starting Vivenu Events Ticket Scrapper v1...")

    if not args.skip_connectivity_check:
        from utils.api_connectivity import check_event_api_reachable

        ok, report = check_event_api_reachable()
        print(report)
        if not ok:
            print("❌ Connectivity check failed; aborting. Use --skip-connectivity-check to bypass.")
            sys.exit(1)

    if args.script:
        # Run specific script
        script_path = f"v1/{args.script}.py"
        if not os.path.exists(script_path):
            print(f"❌ Script not found: {script_path}")
            sys.exit(1)
        
        print(f"📄 Running script: {script_path}")
        os.system(f"python {script_path}")
    else:
        # Run main v1 script
        print("📄 Running main v1 ingestion...")
        os.system("python v1/run_ingest.py")

def run_v2(args):
    """Run v2 system"""
    print("🚀 Starting Vivenu Events Ticket Scrapper v2...")

    if not args.skip_connectivity_check and not args.skip_fetch:
        from utils.api_connectivity import check_event_api_reachable

        ok, report = check_event_api_reachable()
        print(report)
        if not ok:
            print("❌ Connectivity check failed; aborting. Use --skip-connectivity-check to bypass.")
            sys.exit(1)

    # Build command for v2 system
    cmd = ["python", "v2/run_ingest.py"]
    
    if args.debug:
        cmd.append("--debug")
    if args.skip_fetch:
        cmd.append("--skip-fetch")
    if args.pipeline_config:
        cmd.extend(["--pipeline-config", args.pipeline_config])
    if args.pipeline_name:
        cmd.extend(["--pipeline-name", args.pipeline_name])
    
    print(f"📄 Running command: {' '.join(cmd)}")
    os.system(" ".join(cmd))

if __name__ == "__main__":
    main()
