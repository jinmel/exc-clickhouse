"""
YAML Exporter for trading pairs.

Formats trading pairs from exchange adapters into a flat list of TradingPair objects
compatible with the Rust application.
"""

import yaml
from typing import Dict, List, Any

from models import TradingPair


class YAMLExporter:
    """Exports trading pairs to YAML format."""
    
    def __init__(self):
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        """Setup logger for the exporter."""
        import logging
        return logging.getLogger(self.__class__.__name__)
    
    def create_trading_pairs_list(self, trading_pairs: List[TradingPair]) -> List[Dict[str, str]]:
        """
        Convert trading pairs to a flat list of TradingPair dictionaries.
        
        Args:
            trading_pairs: List of TradingPair objects
            
        Returns:
            List of TradingPair dictionaries
        """
        # Convert to list of dictionaries
        pairs_list = []
        
        for pair in trading_pairs:
            pair_dict = {
                'exchange': pair.exchange,
                'trading_type': pair.trading_type,
                'pair': pair.pair,
                'base_asset': pair.base_asset,
                'quote_asset': pair.quote_asset
            }
            pairs_list.append(pair_dict)
        
        # Sort by exchange, then by pair for consistent output
        pairs_list.sort(key=lambda x: (x['exchange'], x['pair']))
        
        self.logger.info(
            f"Created flat trading pairs list: {len(pairs_list)} total pairs"
        )
        
        return pairs_list
    
    def to_yaml_string(self, trading_pairs_list: List[Dict[str, str]]) -> str:
        """
        Convert trading pairs list to YAML string.
        
        Args:
            trading_pairs_list: List of TradingPair dictionaries
            
        Returns:
            YAML formatted string
        """
        # Generate YAML with proper formatting
        yaml_content = yaml.dump(
            trading_pairs_list,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
            width=1000,  # Prevent line wrapping
            indent=2
        )
        
        return yaml_content
    
    def validate_yaml_format(self, yaml_content: str) -> bool:
        """
        Validate that the YAML content is properly formatted.
        
        Args:
            yaml_content: YAML string to validate
            
        Returns:
            True if valid, False otherwise
        """
        try:
            # Parse the YAML to ensure it's valid
            parsed = yaml.safe_load(yaml_content)
            
            # Validate structure
            if not isinstance(parsed, list):
                self.logger.error("YAML root should be a list")
                return False
            
            required_fields = {'exchange', 'trading_type', 'pair', 'base_asset', 'quote_asset'}
            
            for i, item in enumerate(parsed):
                if not isinstance(item, dict):
                    self.logger.error(f"Item {i} should be a dictionary")
                    return False
                
                # Check required fields
                missing_fields = required_fields - set(item.keys())
                if missing_fields:
                    self.logger.error(f"Item {i} missing fields: {missing_fields}")
                    return False
                
                # Validate field types
                for field in required_fields:
                    if not isinstance(item[field], str):
                        self.logger.error(f"Item {i} {field} should be string")
                        return False
            
            self.logger.debug("YAML validation passed")
            return True
            
        except yaml.YAMLError as e:
            self.logger.error(f"YAML parsing error: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Validation error: {e}")
            return False


# Example usage and testing
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    
    # Create sample trading pairs for testing
    sample_pairs = [
        TradingPair(exchange="binance", pair="BTCUSDT", base_asset="BTC", quote_asset="USDT"),
        TradingPair(exchange="binance", pair="ETHUSDT", base_asset="ETH", quote_asset="USDT"),
        TradingPair(exchange="bybit", pair="BTC-USDT", base_asset="BTC", quote_asset="USDT"),
        TradingPair(exchange="okx", pair="BTC-USDT", base_asset="BTC", quote_asset="USDT"),
    ]
    
    exporter = YAMLExporter()
    
    # Test the export process
    print("Testing YAML export...")
    pairs_list = exporter.create_trading_pairs_list(sample_pairs)
    yaml_content = exporter.to_yaml_string(pairs_list)
    
    print("Generated YAML:")
    print(yaml_content)
    
    # Validate
    is_valid = exporter.validate_yaml_format(yaml_content)
    print(f"Validation result: {is_valid}") 