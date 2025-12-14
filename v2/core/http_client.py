"""
HTTP Client Factory and Configuration Management
Provides centralized HTTP client creation with proper configuration and error handling.
"""

import asyncio
import logging
from typing import Optional, Dict, Any, Union
from dataclasses import dataclass
from enum import Enum
import httpx
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


class ClientType(Enum):
    """HTTP client types"""
    HTTPX = "httpx"
    REQUESTS = "requests"


@dataclass
class HTTPConfig:
    """HTTP client configuration"""
    base_url: str
    headers: Dict[str, str]
    timeout: float = 30.0
    connect_timeout: float = 10.0
    read_timeout: float = 30.0
    write_timeout: float = 10.0
    pool_timeout: float = 5.0
    max_connections: int = 100
    max_keepalive_connections: int = 20
    keepalive_expiry: float = 30.0
    retries: int = 3
    http2_enabled: bool = False
    verify_ssl: bool = False
    follow_redirects: bool = True


class HTTPClientError(Exception):
    """Base exception for HTTP client errors"""
    pass


class ConnectionError(HTTPClientError):
    """Connection-related errors"""
    pass


class TimeoutError(HTTPClientError):
    """Timeout-related errors"""
    pass


class RetryExhaustedError(HTTPClientError):
    """All retry attempts exhausted"""
    pass


class HTTPClientFactory:
    """Factory for creating HTTP clients with proper configuration"""
    
    @staticmethod
    def create_config(
        base_url: str,
        headers: Dict[str, str],
        **kwargs
    ) -> HTTPConfig:
        """Create HTTP configuration with defaults"""
        return HTTPConfig(
            base_url=base_url,
            headers=headers,
            **kwargs
        )
    
    @staticmethod
    def create_httpx_client(config: HTTPConfig) -> httpx.AsyncClient:
        """Create configured httpx client"""
        try:
            limits = httpx.Limits(
                max_keepalive_connections=config.max_keepalive_connections,
                max_connections=config.max_connections,
                keepalive_expiry=config.keepalive_expiry
            )
            
            timeout = httpx.Timeout(
                connect=config.connect_timeout,
                read=config.read_timeout,
                write=config.write_timeout,
                pool=config.pool_timeout
            )
            
            transport = httpx.AsyncHTTPTransport(
                retries=config.retries,
                http2=config.http2_enabled
            )
            
            return httpx.AsyncClient(
                headers=config.headers,
                verify=config.verify_ssl,
                timeout=timeout,
                limits=limits,
                transport=transport,
                follow_redirects=config.follow_redirects
            )
            
        except Exception as e:
            logger.error(f"Failed to create httpx client: {e}")
            raise HTTPClientError(f"Failed to create httpx client: {e}")


class RetryStrategy:
    """Retry strategy with exponential backoff"""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 60.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
    
    async def execute(self, func, *args, **kwargs):
        """Execute function with retry logic"""
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                
                if attempt < self.max_retries - 1:
                    delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                    logger.warning(f"Attempt {attempt + 1}/{self.max_retries} failed: {e}")
                    logger.info(f"Retrying in {delay:.2f} seconds...")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"All {self.max_retries} attempts failed")
                    break
        
        raise RetryExhaustedError(f"All retry attempts exhausted. Last error: {last_exception}")


class HTTPClientManager:
    """Manages HTTP client lifecycle and provides high-level operations"""
    
    def __init__(self, config: HTTPConfig):
        self.config = config
        self._client: Optional[httpx.AsyncClient] = None
        self._retry_strategy = RetryStrategy(
            max_retries=config.retries,
            base_delay=1.0
        )
    
    async def _ensure_client(self) -> httpx.AsyncClient:
        """Ensure client exists and is properly configured"""
        if self._client is None or self._client.is_closed:
            try:
                self._client = HTTPClientFactory.create_httpx_client(self.config)
                logger.debug("Created new httpx client")
            except Exception as e:
                logger.error(f"Failed to create client: {e}")
                raise HTTPClientError(f"Failed to create client: {e}")
        
        return self._client
    
    async def get(self, url: str, params: Optional[Dict] = None, **kwargs) -> httpx.Response:
        """Perform GET request with retry logic"""
        async def _get():
            client = await self._ensure_client()
            full_url = f"{self.config.base_url.rstrip('/')}/{url.lstrip('/')}"
            
            # Add small delay to avoid rate limiting
            await asyncio.sleep(0.5)
            
            response = await client.get(full_url, params=params, **kwargs)
            
            if response.status_code >= 400:
                logger.error(f"HTTP {response.status_code}: {response.text}")
                response.raise_for_status()
            
            return response
        
        return await self._retry_strategy.execute(_get)
    
    async def post(self, url: str, data: Optional[Dict] = None, json: Optional[Dict] = None, **kwargs) -> httpx.Response:
        """Perform POST request with retry logic"""
        async def _post():
            client = await self._ensure_client()
            full_url = f"{self.config.base_url.rstrip('/')}/{url.lstrip('/')}"
            
            await asyncio.sleep(0.5)
            
            response = await client.post(full_url, data=data, json=json, **kwargs)
            
            if response.status_code >= 400:
                logger.error(f"HTTP {response.status_code}: {response.text}")
                response.raise_for_status()
            
            return response
        
        return await self._retry_strategy.execute(_post)
    
    async def close(self):
        """Close the HTTP client"""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None
            logger.debug("Closed httpx client")
    
    @asynccontextmanager
    async def session(self):
        """Context manager for HTTP client session"""
        try:
            yield self
        finally:
            await self.close()
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class VivenuHTTPClient:
    """High-level client for Vivenu API operations"""
    
    def __init__(self, token: str, base_url: str):
        self.token = token
        self.base_url = base_url
        
        # Create configuration
        self.config = HTTPClientFactory.create_config(
            base_url=base_url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Origin": "https://vivenu.com",
                "Referer": "https://vivenu.com/"
            },
            retries=3,
            http2_enabled=False,
            verify_ssl=False
        )
        
        self._client_manager: Optional[HTTPClientManager] = None
    
    async def _get_client_manager(self) -> HTTPClientManager:
        """Get or create client manager"""
        if self._client_manager is None:
            self._client_manager = HTTPClientManager(self.config)
        return self._client_manager
    
    async def get_events(self) -> Dict[str, Any]:
        """Get events from API"""
        client_manager = await self._get_client_manager()
        response = await client_manager.get("/events")
        return response.json()
    
    async def get_tickets(self, skip: int = 0, limit: int = 1000) -> Dict[str, Any]:
        """Get tickets from API"""
        client_manager = await self._get_client_manager()
        params = {
            "status": "VALID,DETAILSREQUIRED",
            "skip": skip,
            "top": limit
        }
        response = await client_manager.get("/tickets", params=params)
        return response.json()
    
    async def get_coupon_series(self) -> Dict[str, Any]:
        """Get coupon series from API"""
        client_manager = await self._get_client_manager()
        response = await client_manager.get("/coupon/series")
        return response.json()
    
    async def get_coupons(self, event_id: str, skip: int = 0, limit: int = 1000) -> Dict[str, Any]:
        """Get coupons from API"""
        client_manager = await self._get_client_manager()
        params = {
            "active": "true",
            "skip": skip,
            "top": limit,
            "eventId": event_id
        }
        response = await client_manager.get("/coupon/rich", params=params)
        return response.json()
    
    async def close(self):
        """Close the client"""
        if self._client_manager:
            await self._client_manager.close()
            self._client_manager = None
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
