"""
Coinbase exchange adapter implementation.

Fetches SPOT trading pairs from Coinbase using the /products endpoint.
"""

from typing import List, Dict, Any

# Use absolute imports to avoid relative import issues
try:
    from .models import TradingPair
    from .base_adapter import ExchangeAdapter
except ImportError:
    from models import TradingPair
    from base_adapter import ExchangeAdapter


class CoinbaseAdapter(ExchangeAdapter):
    """
    Coinbase exchange adapter for fetching SPOT trading pairs.
    
    Uses the GET /products endpoint to fetch all trading products
    and filters for active SPOT pairs.
    """
    
    def __init__(self):
        """Initialize Coinbase adapter with API configuration."""
        super().__init__(
            exchange_name="coinbase",
            base_url="https://api.exchange.coinbase.com",
            timeout=30,
            max_retries=3
        )
    
    def get_endpoint(self) -> str:
        """Get the Coinbase products endpoint."""
        return "/products"
    
    def get_params(self) -> Dict[str, Any]:
        """Get query parameters for Coinbase API request."""
        return {}  # No parameters needed for products endpoint
    
    def parse_response(self, raw_data: Dict[str, Any] | List[Dict[str, Any]]) -> List[TradingPair]:
        """
        Parse Coinbase products response into TradingPair objects.
        
        Args:
            raw_data: Raw JSON response from Coinbase API (can be dict or list)
            
        Returns:
            List of TradingPair objects
        """
        trading_pairs = []
        
        # Handle both dict and list responses
        if isinstance(raw_data, dict):
            products = raw_data.get('products', [])
        else:
            products = raw_data
        
        for product in products:
            try:
                # Extract basic product information
                product_id = product.get('id', '')  # e.g., "BTC-USD"
                status = product.get('status', '')
                base_currency = product.get('base_currency', '')
                quote_currency = product.get('quote_currency', '')
                
                # Skip if any required field is missing
                if not all([product_id, base_currency, quote_currency]):
                    self.logger.warning(f"Skipping incomplete product data: {product}")
                    continue
                
                # Create TradingPair object
                trading_pair = TradingPair(
                    exchange=self.exchange_name,
                    trading_type="SPOT",
                    pair=product_id,  # Coinbase uses format like "BTC-USD"
                    base_asset=base_currency,
                    quote_asset=quote_currency
                )
                
                # Store status for filtering
                trading_pair._status = status
                trading_pair._trading_disabled = product.get('trading_disabled', False)
                
                trading_pairs.append(trading_pair)
                
            except Exception as e:
                self.logger.warning(f"Failed to parse product {product.get('id', 'unknown')}: {e}")
                continue
        
        self.logger.info(f"Parsed {len(trading_pairs)} products from Coinbase")
        return trading_pairs
    
    def filter_spot_pairs(self, pairs: List[TradingPair]) -> List[TradingPair]:
        """
        Filter Coinbase trading pairs to include only active SPOT pairs.
        
        Args:
            pairs: List of all trading pairs
            
        Returns:
            List of active SPOT trading pairs
        """
        spot_pairs = []
        
        for pair in pairs:
            try:
                # Check if the product has online status and trading is enabled
                status = getattr(pair, '_status', '')
                trading_disabled = getattr(pair, '_trading_disabled', True)
                
                if status != 'online' or trading_disabled:
                    continue
                
                # Clean up temporary attributes
                if hasattr(pair, '_status'):
                    delattr(pair, '_status')
                if hasattr(pair, '_trading_disabled'):
                    delattr(pair, '_trading_disabled')
                
                spot_pairs.append(pair)
                
            except Exception as e:
                self.logger.warning(f"Failed to filter pair {pair.pair}: {e}")
                continue
        
        self.logger.info(f"Filtered to {len(spot_pairs)} active SPOT pairs from Coinbase")
        return spot_pairs


# Convenience function for testing
async def test_coinbase_adapter():
    """Test function for Coinbase adapter."""
    async with CoinbaseAdapter() as adapter:
        response = await adapter.fetch_trading_pairs()
        print(f"Coinbase: {len(response.trading_pairs)} SPOT pairs")
        
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
    asyncio.run(test_coinbase_adapter()) 