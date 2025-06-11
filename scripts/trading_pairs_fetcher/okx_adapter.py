"""
OKX exchange adapter implementation.

Fetches SPOT trading pairs from OKX using the /api/v5/public/instruments endpoint.
"""

from typing import List, Dict, Any

# Use absolute imports to avoid relative import issues
try:
    from .models import TradingPair
    from .base_adapter import ExchangeAdapter
except ImportError:
    from models import TradingPair
    from base_adapter import ExchangeAdapter


class OKXAdapter(ExchangeAdapter):
    """
    OKX exchange adapter for fetching SPOT trading pairs.
    
    Uses the GET /api/v5/public/instruments endpoint with instType=SPOT
    to fetch all SPOT trading instruments.
    """
    
    def __init__(self):
        """Initialize OKX adapter with API configuration."""
        super().__init__(
            exchange_name="OKX",
            base_url="https://www.okx.com",
            timeout=30,
            max_retries=3
        )
    
    def get_endpoint(self) -> str:
        """Get the OKX public instruments endpoint."""
        return "/api/v5/public/instruments"
    
    def get_params(self) -> Dict[str, Any]:
        """Get query parameters for OKX API request."""
        return {
            "instType": "SPOT"  # Only fetch SPOT instruments
        }
    
    def parse_response(self, raw_data: Dict[str, Any]) -> List[TradingPair]:
        """
        Parse OKX instruments response into TradingPair objects.
        
        Args:
            raw_data: Raw JSON response from OKX API
            
        Returns:
            List of TradingPair objects
        """
        trading_pairs = []
        
        # Extract data from the response
        instruments = raw_data.get('data', [])
        
        for instrument in instruments:
            try:
                # Extract basic instrument information
                inst_id = instrument.get('instId', '')  # e.g., "BTC-USDT"
                state = instrument.get('state', '')
                base_ccy = instrument.get('baseCcy', '')
                quote_ccy = instrument.get('quoteCcy', '')
                
                # Skip if any required field is missing
                if not all([inst_id, base_ccy, quote_ccy]):
                    self.logger.warning(f"Skipping incomplete instrument data: {instrument}")
                    continue
                
                # Create TradingPair object
                trading_pair = TradingPair(
                    exchange=self.exchange_name,
                    trading_type="SPOT",
                    pair=inst_id,  # OKX uses format like "BTC-USDT"
                    base_asset=base_ccy,
                    quote_asset=quote_ccy
                )
                
                # Store state for filtering
                trading_pair._state = state
                
                trading_pairs.append(trading_pair)
                
            except Exception as e:
                self.logger.warning(f"Failed to parse instrument {instrument.get('instId', 'unknown')}: {e}")
                continue
        
        self.logger.info(f"Parsed {len(trading_pairs)} instruments from OKX")
        return trading_pairs
    
    def filter_spot_pairs(self, pairs: List[TradingPair]) -> List[TradingPair]:
        """
        Filter OKX trading pairs to include only live SPOT pairs.
        
        Args:
            pairs: List of all trading pairs
            
        Returns:
            List of live SPOT trading pairs
        """
        spot_pairs = []
        
        for pair in pairs:
            try:
                # Check if the instrument has live state
                state = getattr(pair, '_state', '')
                if state != 'live':
                    continue
                
                # Clean up temporary attributes
                if hasattr(pair, '_state'):
                    delattr(pair, '_state')
                
                spot_pairs.append(pair)
                
            except Exception as e:
                self.logger.warning(f"Failed to filter pair {pair.pair}: {e}")
                continue
        
        self.logger.info(f"Filtered to {len(spot_pairs)} live SPOT pairs from OKX")
        return spot_pairs


# Convenience function for testing
async def test_okx_adapter():
    """Test function for OKX adapter."""
    async with OKXAdapter() as adapter:
        response = await adapter.fetch_trading_pairs()
        print(f"OKX: {len(response.trading_pairs)} SPOT pairs")
        
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
    asyncio.run(test_okx_adapter()) 