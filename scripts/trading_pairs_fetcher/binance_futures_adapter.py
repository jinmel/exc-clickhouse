"""
Binance Futures exchange adapter implementation.

Fetches FUTURES trading pairs from Binance using the /fapi/v1/exchangeInfo endpoint.
"""

from typing import List, Dict, Any

# Use absolute imports to avoid relative import issues
try:
    from .models import TradingPair
    from .base_adapter import ExchangeAdapter
except ImportError:
    from models import TradingPair
    from base_adapter import ExchangeAdapter


class BinanceFuturesAdapter(ExchangeAdapter):
    """
    Binance Futures exchange adapter for fetching FUTURES trading pairs.
    
    Uses the GET /fapi/v1/exchangeInfo endpoint to fetch all futures trading symbols
    and filters for FUTURES pairs with TRADING status.
    """
    
    def __init__(self):
        """Initialize Binance Futures adapter with API configuration."""
        super().__init__(
            exchange_name="binance-futures",
            base_url="https://fapi.binance.com",
            timeout=30,
            max_retries=3
        )
    
    def get_endpoint(self) -> str:
        """Get the Binance Futures exchange info endpoint."""
        return "/fapi/v1/exchangeInfo"
    
    def get_params(self) -> Dict[str, Any]:
        """Get query parameters for Binance Futures API request."""
        return {}  # No parameters needed for exchangeInfo
    
    def parse_response(self, raw_data: Dict[str, Any]) -> List[TradingPair]:
        """
        Parse Binance Futures exchangeInfo response into TradingPair objects.
        
        Args:
            raw_data: Raw JSON response from Binance Futures API
            
        Returns:
            List of TradingPair objects
        """
        trading_pairs = []
        
        # Extract symbols from the response
        symbols = raw_data.get('symbols', [])
        
        for symbol_data in symbols:
            try:
                # Extract basic symbol information
                symbol = symbol_data.get('symbol', '')
                status = symbol_data.get('status', '')
                base_asset = symbol_data.get('baseAsset', '')
                quote_asset = symbol_data.get('quoteAsset', '')
                contract_type = symbol_data.get('contractType', '')
                
                # Skip if any required field is missing
                if not all([symbol, base_asset, quote_asset]):
                    self.logger.warning(f"Skipping incomplete symbol data: {symbol_data}")
                    continue
                
                # Create TradingPair object for futures
                trading_pair = TradingPair(
                    exchange=self.exchange_name,
                    trading_type="FUTURES",
                    pair=symbol,
                    base_asset=base_asset,
                    quote_asset=quote_asset
                )
                
                # Store status and contract info for filtering
                trading_pair._status = status
                trading_pair._contract_type = contract_type
                
                trading_pairs.append(trading_pair)
                
            except Exception as e:
                self.logger.warning(f"Failed to parse symbol {symbol_data.get('symbol', 'unknown')}: {e}")
                continue
        
        self.logger.info(f"Parsed {len(trading_pairs)} symbols from Binance Futures")
        return trading_pairs
    
    def filter_spot_pairs(self, pairs: List[TradingPair]) -> List[TradingPair]:
        """
        Filter Binance Futures trading pairs to include only PERPETUAL futures (no expiry).
        
        Note: This method is named filter_spot_pairs for compatibility with the base class,
        but actually filters for perpetual futures pairs.
        
        Args:
            pairs: List of all trading pairs
            
        Returns:
            List of PERPETUAL FUTURES trading pairs with TRADING status
        """
        futures_pairs = []
        
        for pair in pairs:
            try:
                # Check if the symbol has TRADING status
                status = getattr(pair, '_status', '')
                if status != 'TRADING':
                    continue
                
                # Only include PERPETUAL contracts (exclude CURRENT_QUARTER, NEXT_QUARTER)
                contract_type = getattr(pair, '_contract_type', '')
                if contract_type != 'PERPETUAL':
                    continue
                
                # Clean up temporary attributes
                for attr in ['_status', '_contract_type']:
                    if hasattr(pair, attr):
                        delattr(pair, attr)
                
                futures_pairs.append(pair)
                
            except Exception as e:
                self.logger.warning(f"Failed to filter pair {pair.pair}: {e}")
                continue
        
        self.logger.info(f"Filtered to {len(futures_pairs)} PERPETUAL FUTURES pairs from Binance Futures")
        return futures_pairs


# Convenience function for testing
async def test_binance_futures_adapter():
    """Test function for Binance Futures adapter."""
    async with BinanceFuturesAdapter() as adapter:
        response = await adapter.fetch_trading_pairs()
        print(f"Binance Futures: {len(response.trading_pairs)} FUTURES pairs")
        
        # Show first few pairs
        for pair in response.trading_pairs[:5]:
            print(f"  {pair.pair}: {pair.base_asset}/{pair.quote_asset}")
        
        return response


if __name__ == "__main__":
    import asyncio
    import logging
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Run test
    asyncio.run(test_binance_futures_adapter()) 