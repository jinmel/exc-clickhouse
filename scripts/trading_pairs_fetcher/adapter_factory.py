"""
Exchange adapter factory module.

Provides centralized access to all exchange adapters and utilities for
fetching trading pairs from multiple exchanges concurrently.
"""

import asyncio
import logging
from typing import List, Dict, Type, Optional
from concurrent.futures import ThreadPoolExecutor

# Use absolute imports to avoid relative import issues
try:
    from .models import ExchangeResponse, TradingPair
    from .base_adapter import ExchangeAdapter
    from .binance_adapter import BinanceAdapter
    from .bybit_adapter import BybitAdapter
    from .okx_adapter import OKXAdapter
    from .coinbase_adapter import CoinbaseAdapter
    from .kucoin_adapter import KuCoinAdapter
    from .kraken_adapter import KrakenAdapter
    from .mexc_adapter import MexcAdapter
except ImportError:
    from models import ExchangeResponse, TradingPair
    from base_adapter import ExchangeAdapter
    from binance_adapter import BinanceAdapter
    from bybit_adapter import BybitAdapter
    from okx_adapter import OKXAdapter
    from coinbase_adapter import CoinbaseAdapter
    from kucoin_adapter import KuCoinAdapter
    from kraken_adapter import KrakenAdapter
    from mexc_adapter import MexcAdapter


class AdapterFactory:
    """
    Factory class for creating and managing exchange adapters.
    
    Provides methods to get individual adapters or fetch from multiple exchanges.
    """
    
    # Registry of available adapters
    ADAPTERS: Dict[str, Type[ExchangeAdapter]] = {
        'binance': BinanceAdapter,
        'bybit': BybitAdapter,
        'okx': OKXAdapter,
        'coinbase': CoinbaseAdapter,
        'kucoin': KuCoinAdapter,
        'kraken': KrakenAdapter,
        'mexc': MexcAdapter,
    }
    
    def __init__(self):
        """Initialize the adapter factory."""
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @classmethod
    def get_available_exchanges(cls) -> List[str]:
        """
        Get list of available exchange names.
        
        Returns:
            List of exchange names
        """
        return list(cls.ADAPTERS.keys())
    
    @classmethod
    def create_adapter(cls, exchange_name: str) -> ExchangeAdapter:
        """
        Create an adapter for the specified exchange.
        
        Args:
            exchange_name: Name of the exchange (case-insensitive)
            
        Returns:
            Exchange adapter instance
            
        Raises:
            ValueError: If exchange is not supported
        """
        exchange_key = exchange_name.lower()
        
        if exchange_key not in cls.ADAPTERS:
            available = ', '.join(cls.ADAPTERS.keys())
            raise ValueError(f"Unsupported exchange '{exchange_name}'. Available: {available}")
        
        adapter_class = cls.ADAPTERS[exchange_key]
        return adapter_class()
    
    async def fetch_from_exchange(self, exchange_name: str) -> ExchangeResponse:
        """
        Fetch trading pairs from a single exchange.
        
        Args:
            exchange_name: Name of the exchange
            
        Returns:
            ExchangeResponse with trading pairs and metadata
        """
        try:
            async with self.create_adapter(exchange_name) as adapter:
                response = await adapter.fetch_trading_pairs()
                self.logger.info(f"Fetched {len(response.trading_pairs)} pairs from {exchange_name}")
                return response
        
        except Exception as e:
            self.logger.error(f"Failed to fetch from {exchange_name}: {e}")
            return ExchangeResponse(
                trading_pairs=[],
                exchange_name=exchange_name,
                success=False,
                error_message=str(e)
            )
    
    async def fetch_from_all_exchanges(self, 
                                     exchanges: Optional[List[str]] = None,
                                     max_concurrent: int = 6) -> List[ExchangeResponse]:
        """
        Fetch trading pairs from multiple exchanges concurrently.
        
        Args:
            exchanges: List of exchange names to fetch from (default: all)
            max_concurrent: Maximum number of concurrent requests
            
        Returns:
            List of ExchangeResponse objects
        """
        if exchanges is None:
            exchanges = self.get_available_exchanges()
        
        # Validate exchange names
        invalid_exchanges = [ex for ex in exchanges if ex.lower() not in self.ADAPTERS]
        if invalid_exchanges:
            available = ', '.join(self.ADAPTERS.keys())
            raise ValueError(f"Invalid exchanges: {invalid_exchanges}. Available: {available}")
        
        self.logger.info(f"Fetching trading pairs from {len(exchanges)} exchanges: {', '.join(exchanges)}")
        
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_with_semaphore(exchange_name: str) -> ExchangeResponse:
            async with semaphore:
                return await self.fetch_from_exchange(exchange_name)
        
        # Fetch from all exchanges concurrently
        tasks = [fetch_with_semaphore(exchange) for exchange in exchanges]
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle exceptions and convert to error responses
        final_responses = []
        for i, response in enumerate(responses):
            if isinstance(response, Exception):
                self.logger.error(f"Exception fetching from {exchanges[i]}: {response}")
                final_responses.append(ExchangeResponse(
                    trading_pairs=[],
                    exchange_name=exchanges[i],
                    success=False,
                    error_message=str(response)
                ))
            else:
                final_responses.append(response)
        
        # Log summary
        successful = sum(1 for r in final_responses if r.success)
        total_pairs = sum(len(r.trading_pairs) for r in final_responses)
        
        self.logger.info(f"Completed fetching: {successful}/{len(exchanges)} exchanges successful, "
                        f"{total_pairs} total pairs")
        
        return final_responses
    
    def get_all_trading_pairs(self, responses: List[ExchangeResponse]) -> List[TradingPair]:
        """
        Extract all trading pairs from exchange responses.
        
        Args:
            responses: List of exchange responses
            
        Returns:
            List of all trading pairs combined
        """
        all_pairs = []
        
        for response in responses:
            if response.success:
                all_pairs.extend(response.trading_pairs)
            else:
                self.logger.warning(f"Skipping failed response from {response.exchange_name}")
        
        return all_pairs
    
    def get_summary_stats(self, responses: List[ExchangeResponse]) -> Dict[str, any]:
        """
        Get summary statistics from exchange responses.
        
        Args:
            responses: List of exchange responses
            
        Returns:
            Dictionary with summary statistics
        """
        successful_exchanges = [r for r in responses if r.success]
        failed_exchanges = [r for r in responses if not r.success]
        
        stats = {
            'total_exchanges': len(responses),
            'successful_exchanges': len(successful_exchanges),
            'failed_exchanges': len(failed_exchanges),
            'total_pairs': sum(len(r.trading_pairs) for r in successful_exchanges),
            'exchange_breakdown': {
                r.exchange_name: len(r.trading_pairs) 
                for r in successful_exchanges
            },
            'failed_exchange_names': [r.exchange_name for r in failed_exchanges],
            'errors': {
                r.exchange_name: r.error_message 
                for r in failed_exchanges if r.error_message
            }
        }
        
        return stats


# Convenience functions
async def fetch_all_exchanges(exchanges: Optional[List[str]] = None) -> List[ExchangeResponse]:
    """
    Convenience function to fetch from all exchanges.
    
    Args:
        exchanges: List of exchange names (default: all available)
        
    Returns:
        List of ExchangeResponse objects
    """
    factory = AdapterFactory()
    return await factory.fetch_from_all_exchanges(exchanges)


async def fetch_single_exchange(exchange_name: str) -> ExchangeResponse:
    """
    Convenience function to fetch from a single exchange.
    
    Args:
        exchange_name: Name of the exchange
        
    Returns:
        ExchangeResponse object
    """
    factory = AdapterFactory()
    return await factory.fetch_from_exchange(exchange_name)


# Demo function
async def demo_all_adapters():
    """Demo function showing how to use all adapters."""
    logging.basicConfig(level=logging.INFO)
    
    factory = AdapterFactory()
    
    print("Available exchanges:", factory.get_available_exchanges())
    print()
    
    # Fetch from all exchanges
    responses = await factory.fetch_from_all_exchanges()
    
    # Show results
    stats = factory.get_summary_stats(responses)
    print("Summary Statistics:")
    print(f"  Total exchanges: {stats['total_exchanges']}")
    print(f"  Successful: {stats['successful_exchanges']}")
    print(f"  Failed: {stats['failed_exchanges']}")
    print(f"  Total pairs: {stats['total_pairs']}")
    print()
    
    print("Exchange breakdown:")
    for exchange, count in stats['exchange_breakdown'].items():
        print(f"  {exchange}: {count} pairs")
    print()
    
    if stats['failed_exchange_names']:
        print("Failed exchanges:")
        for exchange in stats['failed_exchange_names']:
            error = stats['errors'].get(exchange, 'Unknown error')
            print(f"  {exchange}: {error}")
        print()
    
    # Show sample pairs from each successful exchange
    print("Sample pairs from each exchange:")
    for response in responses:
        if response.success and response.trading_pairs:
            pairs = response.trading_pairs[:3]  # First 3 pairs
            print(f"  {response.exchange_name}:")
            for pair in pairs:
                print(f"    {pair.pair}: {pair.base_asset}/{pair.quote_asset}")
    
    return responses


if __name__ == "__main__":
    asyncio.run(demo_all_adapters()) 