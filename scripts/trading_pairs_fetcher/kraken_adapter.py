"""
Kraken exchange adapter implementation.

Fetches SPOT trading pairs from Kraken using the /0/public/AssetPairs endpoint.
Handles Kraken's unique asset naming conventions with proper mapping.
"""

from typing import List, Dict, Any

# Use absolute imports to avoid relative import issues
try:
    from .models import TradingPair
    from .base_adapter import ExchangeAdapter
except ImportError:
    from models import TradingPair
    from base_adapter import ExchangeAdapter


class KrakenAdapter(ExchangeAdapter):
    """
    Kraken exchange adapter for fetching SPOT trading pairs.
    
    Uses the GET /0/public/AssetPairs endpoint to fetch all trading pairs.
    Handles Kraken's unique asset naming conventions and filters for SPOT pairs.
    """
    
    # Kraken asset name mapping (Kraken names -> standard names)
    ASSET_NAME_MAP = {
        'XXBT': 'BTC',
        'XBT': 'BTC',
        'XETH': 'ETH',
        'XLTC': 'LTC',
        'XXRP': 'XRP',
        'XXLM': 'XLM',
        'XETC': 'ETC',
        'XREP': 'REP',
        'XXMR': 'XMR',
        'XZEC': 'ZEC',
        'ZUSD': 'USD',
        'ZEUR': 'EUR',
        'ZGBP': 'GBP',
        'ZJPY': 'JPY',
        'ZCAD': 'CAD',
        'ZAUD': 'AUD',
        'USDT': 'USDT',
        'USDC': 'USDC',
    }
    
    def __init__(self):
        """Initialize Kraken adapter with API configuration."""
        super().__init__(
            exchange_name="Kraken",
            base_url="https://api.kraken.com",
            timeout=30,
            max_retries=3
        )
    
    def get_endpoint(self) -> str:
        """Get the Kraken asset pairs endpoint."""
        return "/0/public/AssetPairs"
    
    def get_params(self) -> Dict[str, Any]:
        """Get query parameters for Kraken API request."""
        return {}  # No parameters needed for AssetPairs endpoint
    
    def _normalize_asset_name(self, asset_name: str) -> str:
        """
        Normalize Kraken asset names to standard format.
        
        Args:
            asset_name: Kraken asset name
            
        Returns:
            Normalized asset name
        """
        # Remove common prefixes/suffixes
        asset = asset_name.upper()
        
        # Check direct mapping first
        if asset in self.ASSET_NAME_MAP:
            return self.ASSET_NAME_MAP[asset]
        
        # Remove .S suffix (for staked assets)
        if asset.endswith('.S'):
            asset = asset[:-2]
        
        # Remove .M suffix (for margin assets)
        if asset.endswith('.M'):
            asset = asset[:-2]
        
        # Try mapping again after removing suffixes
        if asset in self.ASSET_NAME_MAP:
            return self.ASSET_NAME_MAP[asset]
        
        # For assets starting with X or Z, try removing the prefix
        if len(asset) > 3 and asset[0] in ['X', 'Z']:
            base_asset = asset[1:]
            if base_asset in self.ASSET_NAME_MAP:
                return self.ASSET_NAME_MAP[base_asset]
        
        # If no mapping found, return the original (after removing prefixes)
        if len(asset) > 3 and asset[0] in ['X', 'Z'] and asset not in ['XRP', 'XLM']:
            return asset[1:]
        
        return asset
    
    def parse_response(self, raw_data: Dict[str, Any]) -> List[TradingPair]:
        """
        Parse Kraken AssetPairs response into TradingPair objects.
        
        Args:
            raw_data: Raw JSON response from Kraken API
            
        Returns:
            List of TradingPair objects
        """
        trading_pairs = []
        
        # Extract result from the response
        result = raw_data.get('result', {})
        
        for pair_name, pair_data in result.items():
            try:
                # Skip if this is not a valid pair data
                if not isinstance(pair_data, dict):
                    continue
                
                # Extract pair information
                altname = pair_data.get('altname', '')
                base = pair_data.get('base', '')
                quote = pair_data.get('quote', '')
                status = pair_data.get('status', '')
                
                # Skip if any required field is missing
                if not all([pair_name, base, quote]):
                    self.logger.warning(f"Skipping incomplete pair data: {pair_name}")
                    continue
                
                # Normalize asset names
                base_asset = self._normalize_asset_name(base)
                quote_asset = self._normalize_asset_name(quote)
                
                # Use altname if available, otherwise use pair_name
                display_name = altname if altname else pair_name
                
                # Create TradingPair object
                trading_pair = TradingPair(
                    exchange=self.exchange_name,
                    trading_type="SPOT",
                    pair=display_name,
                    base_asset=base_asset,
                    quote_asset=quote_asset
                )
                
                # Store status for filtering
                trading_pair._status = status
                
                trading_pairs.append(trading_pair)
                
            except Exception as e:
                self.logger.warning(f"Failed to parse pair {pair_name}: {e}")
                continue
        
        self.logger.info(f"Parsed {len(trading_pairs)} pairs from Kraken")
        return trading_pairs
    
    def filter_spot_pairs(self, pairs: List[TradingPair]) -> List[TradingPair]:
        """
        Filter Kraken trading pairs to include only online SPOT pairs.
        
        Args:
            pairs: List of all trading pairs
            
        Returns:
            List of online SPOT trading pairs
        """
        spot_pairs = []
        
        for pair in pairs:
            try:
                # Check if the pair has online status
                status = getattr(pair, '_status', '')
                if status != 'online':
                    continue
                
                # Filter out obvious non-SPOT pairs
                pair_name = pair.pair.upper()
                
                # Skip futures, perpetuals, and other derivatives
                if any(suffix in pair_name for suffix in ['.FUT', '.PERP', '_PERP', '.F']):
                    continue
                
                # Skip margin trading pairs (they usually have .M suffix)
                if '.M' in pair_name:
                    continue
                
                # Clean up temporary attributes
                if hasattr(pair, '_status'):
                    delattr(pair, '_status')
                
                spot_pairs.append(pair)
                
            except Exception as e:
                self.logger.warning(f"Failed to filter pair {pair.pair}: {e}")
                continue
        
        self.logger.info(f"Filtered to {len(spot_pairs)} online SPOT pairs from Kraken")
        return spot_pairs


# Convenience function for testing
async def test_kraken_adapter():
    """Test function for Kraken adapter."""
    async with KrakenAdapter() as adapter:
        response = await adapter.fetch_trading_pairs()
        print(f"Kraken: {len(response.trading_pairs)} SPOT pairs")
        
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
    asyncio.run(test_kraken_adapter()) 