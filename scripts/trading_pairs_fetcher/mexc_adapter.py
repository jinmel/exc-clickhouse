"""
MEXC exchange adapter implementation.

Fetches SPOT trading pairs from MEXC using the /api/v3/exchangeInfo endpoint.
"""

from typing import List, Dict, Any

# Use absolute imports to avoid relative import issues
try:
    from .models import TradingPair
    from .base_adapter import ExchangeAdapter
except ImportError:
    from models import TradingPair
    from base_adapter import ExchangeAdapter


class MexcAdapter(ExchangeAdapter):
    """
    MEXC exchange adapter for fetching SPOT trading pairs.
    
    Uses the GET /api/v3/exchangeInfo endpoint to fetch all trading symbols
    and filters for SPOT pairs with ENABLED status.
    """
    
    def __init__(self):
        """Initialize MEXC adapter with API configuration."""
        super().__init__(
            exchange_name="MEXC",
            base_url="https://api.mexc.com",
            timeout=30,
            max_retries=3
        )
    
    def get_endpoint(self) -> str:
        """Get the MEXC exchange info endpoint."""
        return "/api/v3/exchangeInfo"
    
    def get_params(self) -> Dict[str, Any]:
        """Get query parameters for MEXC API request."""
        return {}  # No parameters needed for exchangeInfo
    
    def parse_response(self, raw_data: Dict[str, Any]) -> List[TradingPair]:
        """
        Parse MEXC exchangeInfo response into TradingPair objects.
        
        Args:
            raw_data: Raw JSON response from MEXC API
            
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
                
                # Skip if any required field is missing
                if not all([symbol, base_asset, quote_asset]):
                    self.logger.warning(f"Skipping incomplete symbol data: {symbol_data}")
                    continue
                
                # Create TradingPair object
                trading_pair = TradingPair(
                    exchange=self.exchange_name,
                    trading_type="SPOT",  # Will be validated by the model
                    pair=symbol,
                    base_asset=base_asset,
                    quote_asset=quote_asset
                )
                
                # Store status for filtering
                trading_pair._status = status
                
                # Check if spot trading is allowed
                is_spot_trading_allowed = symbol_data.get('isSpotTradingAllowed', False)
                trading_pair._is_spot_trading_allowed = is_spot_trading_allowed
                
                trading_pairs.append(trading_pair)
                
            except Exception as e:
                self.logger.warning(f"Failed to parse symbol {symbol_data.get('symbol', 'unknown')}: {e}")
                continue
        
        self.logger.info(f"Parsed {len(trading_pairs)} symbols from MEXC")
        return trading_pairs
    
    def filter_spot_pairs(self, pairs: List[TradingPair]) -> List[TradingPair]:
        """
        Filter MEXC trading pairs to include only active SPOT pairs.
        
        Args:
            pairs: List of all trading pairs
            
        Returns:
            List of SPOT trading pairs with ENABLED status
        """
        spot_pairs = []
        
        for pair in pairs:
            try:
                # Check if the symbol has ENABLED status
                status = getattr(pair, '_status', '')
                if status != 'ENABLED':
                    continue
                
                # Check if SPOT trading is allowed
                is_spot_allowed = getattr(pair, '_is_spot_trading_allowed', False)
                if not is_spot_allowed:
                    continue
                
                # Clean up temporary attributes
                for attr in ['_status', '_is_spot_trading_allowed']:
                    if hasattr(pair, attr):
                        delattr(pair, attr)
                
                spot_pairs.append(pair)
                
            except Exception as e:
                self.logger.warning(f"Failed to filter pair {pair.pair}: {e}")
                continue
        
        self.logger.info(f"Filtered to {len(spot_pairs)} SPOT pairs from MEXC")
        return spot_pairs


# Convenience function for testing
async def test_mexc_adapter():
    """Test function for MEXC adapter."""
    async with MexcAdapter() as adapter:
        response = await adapter.fetch_trading_pairs()
        print(f"MEXC: {len(response.trading_pairs)} SPOT pairs")
        
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
    asyncio.run(test_mexc_adapter())