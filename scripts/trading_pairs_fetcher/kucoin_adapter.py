"""
KuCoin exchange adapter implementation.

Fetches SPOT trading pairs from KuCoin using the /api/v1/symbols endpoint.
"""

from typing import List, Dict, Any

# Use absolute imports to avoid relative import issues
try:
    from .models import TradingPair
    from .base_adapter import ExchangeAdapter
except ImportError:
    from models import TradingPair
    from base_adapter import ExchangeAdapter


class KuCoinAdapter(ExchangeAdapter):
    """
    KuCoin exchange adapter for fetching SPOT trading pairs.
    
    Uses the GET /api/v1/symbols endpoint to fetch all trading symbols
    and filters for SPOT pairs that are enabled for trading.
    """
    
    def __init__(self):
        """Initialize KuCoin adapter with API configuration."""
        super().__init__(
            exchange_name="kucoin",
            base_url="https://api.kucoin.com",
            timeout=30,
            max_retries=3
        )
    
    def get_endpoint(self) -> str:
        """Get the KuCoin symbols endpoint."""
        return "/api/v1/symbols"
    
    def get_params(self) -> Dict[str, Any]:
        """Get query parameters for KuCoin API request."""
        return {}  # No parameters needed for symbols endpoint
    
    def parse_response(self, raw_data: Dict[str, Any]) -> List[TradingPair]:
        """
        Parse KuCoin symbols response into TradingPair objects.
        
        Args:
            raw_data: Raw JSON response from KuCoin API
            
        Returns:
            List of TradingPair objects
        """
        trading_pairs = []
        
        # Extract data from the response
        symbols = raw_data.get('data', [])
        
        for symbol_data in symbols:
            try:
                # Extract basic symbol information
                symbol = symbol_data.get('symbol', '')  # e.g., "BTC-USDT"
                name = symbol_data.get('name', '')
                base_currency = symbol_data.get('baseCurrency', '')
                quote_currency = symbol_data.get('quoteCurrency', '')
                enable_trading = symbol_data.get('enableTrading', False)
                
                # Skip if any required field is missing
                if not all([symbol, base_currency, quote_currency]):
                    self.logger.warning(f"Skipping incomplete symbol data: {symbol_data}")
                    continue
                
                # Create TradingPair object
                trading_pair = TradingPair(
                    exchange=self.exchange_name,
                    trading_type="SPOT",
                    pair=symbol,  # KuCoin uses format like "BTC-USDT"
                    base_asset=base_currency,
                    quote_asset=quote_currency
                )
                
                # Store trading status for filtering
                trading_pair._enable_trading = enable_trading
                
                trading_pairs.append(trading_pair)
                
            except Exception as e:
                self.logger.warning(f"Failed to parse symbol {symbol_data.get('symbol', 'unknown')}: {e}")
                continue
        
        self.logger.info(f"Parsed {len(trading_pairs)} symbols from KuCoin")
        return trading_pairs
    
    def filter_spot_pairs(self, pairs: List[TradingPair]) -> List[TradingPair]:
        """
        Filter KuCoin trading pairs to include only enabled SPOT pairs.
        
        Args:
            pairs: List of all trading pairs
            
        Returns:
            List of enabled SPOT trading pairs
        """
        spot_pairs = []
        
        for pair in pairs:
            try:
                # Check if trading is enabled
                enable_trading = getattr(pair, '_enable_trading', False)
                if not enable_trading:
                    continue
                
                # Clean up temporary attributes
                if hasattr(pair, '_enable_trading'):
                    delattr(pair, '_enable_trading')
                
                spot_pairs.append(pair)
                
            except Exception as e:
                self.logger.warning(f"Failed to filter pair {pair.pair}: {e}")
                continue
        
        self.logger.info(f"Filtered to {len(spot_pairs)} enabled SPOT pairs from KuCoin")
        return spot_pairs


# Convenience function for testing
async def test_kucoin_adapter():
    """Test function for KuCoin adapter."""
    async with KuCoinAdapter() as adapter:
        response = await adapter.fetch_trading_pairs()
        print(f"KuCoin: {len(response.trading_pairs)} SPOT pairs")
        
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
    asyncio.run(test_kucoin_adapter()) 