#!/usr/bin/env python3
"""
Network connectivity test script for Docker cron environment
This script helps diagnose network connectivity issues in the containerized cron environment.
"""

import os
import sys
import subprocess
import asyncio
import httpx
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_basic_connectivity():
    """Test basic network connectivity"""
    logger.info("Testing basic network connectivity...")
    
    # Test DNS resolution
    try:
        result = subprocess.run(['nslookup', 'google.com'], capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            logger.info("✓ DNS resolution working")
        else:
            logger.error("✗ DNS resolution failed")
            logger.error(f"Error: {result.stderr}")
    except Exception as e:
        logger.error(f"✗ DNS test failed: {e}")
    
    # Test ping
    try:
        result = subprocess.run(['ping', '-c', '3', '8.8.8.8'], capture_output=True, text=True, timeout=15)
        if result.returncode == 0:
            logger.info("✓ Basic connectivity working")
        else:
            logger.error("✗ Basic connectivity failed")
    except Exception as e:
        logger.error(f"✗ Ping test failed: {e}")

def test_https_connectivity():
    """Test HTTPS connectivity to common endpoints"""
    logger.info("Testing HTTPS connectivity...")
    
    test_urls = [
        "https://httpbin.org/get",
        "https://api.github.com",
        "https://www.google.com"
    ]
    
    for url in test_urls:
        try:
            result = subprocess.run(['curl', '-I', '--connect-timeout', '10', url], 
                                  capture_output=True, text=True, timeout=15)
            if result.returncode == 0:
                logger.info(f"✓ HTTPS connectivity to {url} working")
            else:
                logger.error(f"✗ HTTPS connectivity to {url} failed")
                logger.error(f"Error: {result.stderr}")
        except Exception as e:
            logger.error(f"✗ HTTPS test for {url} failed: {e}")

async def test_httpx_connectivity():
    """Test httpx connectivity with various configurations"""
    logger.info("Testing httpx connectivity...")
    
    test_url = "https://httpbin.org/get"
    
    # Test 1: Basic httpx client
    try:
        async with httpx.AsyncClient(verify=False, timeout=30.0) as client:
            response = await client.get(test_url)
            if response.status_code == 200:
                logger.info("✓ Basic httpx client working")
            else:
                logger.error(f"✗ Basic httpx client failed with status {response.status_code}")
    except Exception as e:
        logger.error(f"✗ Basic httpx client failed: {e}")
    
    # Test 2: httpx with retry transport
    try:
        transport = httpx.AsyncHTTPTransport(
            retries=3,
            http2=False
        )
        async with httpx.AsyncClient(
            verify=False, 
            timeout=30.0,
            transport=transport
        ) as client:
            response = await client.get(test_url)
            if response.status_code == 200:
                logger.info("✓ httpx with retry transport working")
            else:
                logger.error(f"✗ httpx with retry transport failed with status {response.status_code}")
    except Exception as e:
        logger.error(f"✗ httpx with retry transport failed: {e}")
    
    # Test 3: httpx with connection limits
    try:
        limits = httpx.Limits(
            max_keepalive_connections=20,
            max_connections=100,
            keepalive_expiry=30.0
        )
        timeout = httpx.Timeout(
            connect=10.0,
            read=30.0,
            write=10.0,
            pool=5.0
        )
        async with httpx.AsyncClient(
            verify=False,
            timeout=timeout,
            limits=limits
        ) as client:
            response = await client.get(test_url)
            if response.status_code == 200:
                logger.info("✓ httpx with connection limits working")
            else:
                logger.error(f"✗ httpx with connection limits failed with status {response.status_code}")
    except Exception as e:
        logger.error(f"✗ httpx with connection limits failed: {e}")

def test_environment_variables():
    """Test environment variables that might affect connectivity"""
    logger.info("Checking environment variables...")
    
    env_vars = [
        'EVENT_API_BASE_URL',
        'PYTHONPATH',
        'PYTHONUNBUFFERED',
        'SSL_VERIFY',
        'REQUESTS_CA_BUNDLE',
        'CURL_CA_BUNDLE'
    ]
    
    for var in env_vars:
        value = os.getenv(var)
        if value:
            logger.info(f"✓ {var}={value}")
        else:
            logger.info(f"- {var} not set")

async def main():
    """Main test function"""
    logger.info("=" * 60)
    logger.info("Network Connectivity Test for Docker Cron Environment")
    logger.info(f"Test started at: {datetime.now()}")
    logger.info("=" * 60)
    
    # Test environment variables
    test_environment_variables()
    logger.info("")
    
    # Test basic connectivity
    test_basic_connectivity()
    logger.info("")
    
    # Test HTTPS connectivity
    test_https_connectivity()
    logger.info("")
    
    # Test httpx connectivity
    await test_httpx_connectivity()
    logger.info("")
    
    logger.info("=" * 60)
    logger.info("Network connectivity test completed")
    logger.info("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
