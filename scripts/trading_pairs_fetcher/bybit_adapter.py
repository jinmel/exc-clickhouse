"""
Bybit exchange adapter implementation.

Fetches SPOT trading pairs from Bybit using the /v5/market/instruments-info endpoint.
"""

from typing import List, Dict, Any

# Use absolute imports to avoid relative import issues
try:
    from .models import TradingPair
    from .base_adapter import ExchangeAdapter
except ImportError:
    from models import TradingPair
    from base_adapter import ExchangeAdapter


class BybitAdapter(ExchangeAdapter):
    """
    Bybit exchange adapter for fetching SPOT trading pairs.
    
    Uses the GET /v5/market/instruments-info endpoint with category=spot
    to fetch all SPOT trading symbols.
    """
    
    def __init__(self):
        """Initialize Bybit adapter with API configuration."""
        super().__init__(
            exchange_name="bybit",
            base_url="https://api.bybit.com",
            timeout=30,
            max_retries=3
        )
    
    def get_endpoint(self) -> str:
        """Get the Bybit instruments info endpoint."""
        return "/v5/market/instruments-info"
    
    def get_params(self) -> Dict[str, Any]:
        """Get query parameters for Bybit API request."""
        return {
            "category": "spot"  # Only fetch SPOT instruments
        }
    
    def parse_response(self, raw_data: Dict[str, Any]) -> List[TradingPair]:
        """
        Parse Bybit instruments-info response into TradingPair objects.
        
        Args:
            raw_data: Raw JSON response from Bybit API
            
        Returns:
            List of TradingPair objects
        """
        trading_pairs = []
        
        # Extract result from the response
        result = raw_data.get('result', {})
        instruments = result.get('list', [])
        
        for instrument in instruments:
            try:
                # Extract basic instrument information
                symbol = instrument.get('symbol', '')
                status = instrument.get('status', '')
                base_coin = instrument.get('baseCoin', '')
                quote_coin = instrument.get('quoteCoin', '')
                
                # Skip if any required field is missing
                if not all([symbol, base_coin, quote_coin]):
                    self.logger.warning(f"Skipping incomplete instrument data: {instrument}")
                    continue
                
                # Create TradingPair object
                trading_pair = TradingPair(
                    exchange=self.exchange_name,
                    trading_type="SPOT",
                    pair=symbol,
                    base_asset=base_coin,
                    quote_asset=quote_coin
                )
                
                # Store status for filtering
                trading_pair._status = status
                
                trading_pairs.append(trading_pair)
                
            except Exception as e:
                self.logger.warning(f"Failed to parse instrument {instrument.get('symbol', 'unknown')}: {e}")
                continue
        
        self.logger.info(f"Parsed {len(trading_pairs)} instruments from Bybit")
        return trading_pairs
    
    def filter_spot_pairs(self, pairs: List[TradingPair]) -> List[TradingPair]:
        """
        Filter Bybit trading pairs to include only active SPOT pairs.
        
        Args:
            pairs: List of all trading pairs
            
        Returns:
            List of active SPOT trading pairs
        """
        spot_pairs = []
        
        for pair in pairs:
            try:
                # Check if the instrument has Trading status
                status = getattr(pair, '_status', '')
                if status != 'Trading':
                    continue
                
                # Clean up temporary attributes
                if hasattr(pair, '_status'):
                    delattr(pair, '_status')
                
                spot_pairs.append(pair)
                
            except Exception as e:
                self.logger.warning(f"Failed to filter pair {pair.pair}: {e}")
                continue
        
        self.logger.info(f"Filtered to {len(spot_pairs)} active SPOT pairs from Bybit")
        return spot_pairs


# Convenience function for testing
async def test_bybit_adapter():
    """Test function for Bybit adapter."""
    async with BybitAdapter() as adapter:
        response = await adapter.fetch_trading_pairs()
        print(f"Bybit: {len(response.trading_pairs)} SPOT pairs")
        
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
    asyncio.run(test_bybit_adapter()) 