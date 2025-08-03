"""
Base adapter class for exchange API implementations.

This module defines the abstract base class that all exchange adapters must inherit from,
providing a consistent interface for fetching and parsing trading pairs data.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import aiohttp
import asyncio
import logging
from datetime import datetime

# Use absolute imports to avoid relative import issues
try:
    from .models import TradingPair, ExchangeResponse
except ImportError:
    from models import TradingPair, ExchangeResponse


class ExchangeAdapter(ABC):
    """
    Abstract base class for exchange API adapters.
    
    All exchange adapters must inherit from this class and implement the abstract methods.
    """
    
    def __init__(self, 
                 exchange_name: str,
                 base_url: str,
                 timeout: int = 30,
                 max_retries: int = 3):
        """
        Initialize the exchange adapter.
        
        Args:
            exchange_name: Name of the exchange (e.g., "Binance")
            base_url: Base URL for the exchange API
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
        """
        self.exchange_name = exchange_name
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        
        # Session will be initialized when needed
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self._ensure_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    async def _ensure_session(self):
        """Ensure aiohttp session is initialized."""
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit=100,
                limit_per_host=30,
                ttl_dns_cache=300,
                use_dns_cache=True
            )
            
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers={
                    'User-Agent': 'TradingPairsFetcher/1.0',
                    'Accept': 'application/json',
                    'Content-Type': 'application/json'
                }
            )
    
    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
    
    @abstractmethod
    def get_endpoint(self) -> str:
        """
        Get the API endpoint for fetching trading pairs.
        
        Returns:
            The endpoint path (will be combined with base_url)
        """
        pass
    
    @abstractmethod
    def get_params(self) -> Dict[str, Any]:
        """
        Get query parameters for the API request.
        
        Returns:
            Dictionary of query parameters
        """
        pass
    
    @abstractmethod
    def parse_response(self, raw_data: Dict[str, Any]) -> List[TradingPair]:
        """
        Parse the raw API response into TradingPair objects.
        
        Args:
            raw_data: Raw JSON response from the API
            
        Returns:
            List of TradingPair objects
        """
        pass
    
    @abstractmethod
    def filter_spot_pairs(self, pairs: List[TradingPair]) -> List[TradingPair]:
        """
        Filter trading pairs to include only SPOT pairs.
        
        Args:
            pairs: List of all trading pairs
            
        Returns:
            List of SPOT trading pairs only
        """
        pass
    
    async def fetch_raw_data(self) -> Dict[str, Any]:
        """
        Fetch raw data from the exchange API with retry logic.
        
        Returns:
            Raw JSON response from the API
            
        Raises:
            aiohttp.ClientError: If all retry attempts fail
        """
        await self._ensure_session()
        
        url = f"{self.base_url.rstrip('/')}/{self.get_endpoint().lstrip('/')}"
        params = self.get_params()
        
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                self.logger.debug(f"Fetching data from {self.exchange_name}, attempt {attempt + 1}")
                
                async with self._session.get(url, params=params) as response:
                    # Log response details
                    self.logger.debug(f"Response status: {response.status}, URL: {response.url}")
                    
                    # Handle different HTTP status codes
                    if response.status == 200:
                        data = await response.json()
                        self.logger.info(f"Successfully fetched data from {self.exchange_name}")
                        return data
                    
                    elif response.status == 429:  # Rate limit
                        retry_after = int(response.headers.get('Retry-After', 60))
                        self.logger.warning(f"Rate limited by {self.exchange_name}, waiting {retry_after}s")
                        if attempt < self.max_retries:
                            await asyncio.sleep(retry_after)
                            continue
                    
                    elif response.status >= 500:  # Server error
                        self.logger.warning(f"Server error from {self.exchange_name}: {response.status}")
                        if attempt < self.max_retries:
                            await asyncio.sleep(2 ** attempt)  # Exponential backoff
                            continue
                    
                    # For other status codes, raise an error
                    response.raise_for_status()
            
            except aiohttp.ClientError as e:
                last_exception = e
                self.logger.warning(f"Request failed for {self.exchange_name}: {e}")
                
                if attempt < self.max_retries:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    continue
            
            except Exception as e:
                last_exception = e
                self.logger.error(f"Unexpected error for {self.exchange_name}: {e}")
                break
        
        # If we get here, all retries failed
        error_msg = f"Failed to fetch data from {self.exchange_name} after {self.max_retries + 1} attempts"
        if last_exception:
            error_msg += f": {last_exception}"
        
        self.logger.error(error_msg)
        raise aiohttp.ClientError(error_msg)
    
    async def fetch_trading_pairs(self) -> ExchangeResponse:
        """
        Fetch and parse trading pairs from the exchange.
        
        Returns:
            ExchangeResponse with trading pairs and metadata
        """
        start_time = datetime.utcnow()
        
        try:
            # Fetch raw data
            raw_data = await self.fetch_raw_data()
            
            # Parse response
            all_pairs = self.parse_response(raw_data)
            
            # Filter for SPOT pairs only
            spot_pairs = self.filter_spot_pairs(all_pairs)
            
            # Create response
            response = ExchangeResponse(
                trading_pairs=spot_pairs,
                exchange_name=self.exchange_name,
                fetch_timestamp=start_time,
                success=True
            )
            
            self.logger.info(f"Successfully processed {len(spot_pairs)} trading pairs from {self.exchange_name}")
            return response
        
        except Exception as e:
            self.logger.error(f"Failed to fetch trading pairs from {self.exchange_name}: {e}")
            
            # Return error response
            return ExchangeResponse(
                trading_pairs=[],
                exchange_name=self.exchange_name,
                fetch_timestamp=start_time,
                success=False,
                error_message=str(e)
            )
    
    def __repr__(self) -> str:
        """String representation of the adapter."""
        return f"{self.__class__.__name__}(exchange_name='{self.exchange_name}', base_url='{self.base_url}')" 