"""
Unit tests for trading pairs fetcher models and YAML exporter.

Tests ensure compatibility with Rust TradingPair structure and symbols.yaml format.
"""

import pytest
import yaml
from typing import List

# Handle both relative and absolute imports
try:
    from .models import (
        TradingPair, 
        SymbolsConfigEntry, 
        SymbolsConfig, 
        ExchangeResponse,
        FetchSummary,
        ExchangeName
    )
    from .yaml_exporter import YAMLExporter, create_sample_yaml
except ImportError:
    from models import (
        TradingPair, 
        SymbolsConfigEntry, 
        SymbolsConfig, 
        ExchangeResponse,
        FetchSummary,
        ExchangeName
    )
    from yaml_exporter import YAMLExporter, create_sample_yaml


class TestTradingPair:
    """Test the TradingPair model."""
    
    def test_trading_pair_creation(self):
        """Test creating a valid TradingPair."""
        pair = TradingPair(
            exchange="binance",
            pair="BTCUSDT",
            base_asset="BTC",
            quote_asset="USDT"
        )
        
        assert pair.exchange == "binance"
        assert pair.trading_type == "SPOT"  # Default value
        assert pair.pair == "BTCUSDT"
        assert pair.base_asset == "BTC"
        assert pair.quote_asset == "USDT"
    
    def test_trading_pair_with_explicit_trading_type(self):
        """Test creating TradingPair with explicit SPOT trading type."""
        pair = TradingPair(
            exchange="bybit",
            trading_type="SPOT",
            pair="ETHUSDT",
            base_asset="ETH",
            quote_asset="USDT"
        )
        
        assert pair.trading_type == "SPOT"
    
    def test_trading_pair_invalid_trading_type(self):
        """Test that invalid trading_type raises ValueError."""
        with pytest.raises(ValueError, match="trading_type must be 'SPOT'"):
            TradingPair(
                exchange="binance",
                trading_type="FUTURES",  # Invalid
                pair="BTCUSDT",
                base_asset="BTC",
                quote_asset="USDT"
            )
    
    def test_trading_pair_empty_pair(self):
        """Test that empty pair raises ValueError."""
        with pytest.raises(ValueError, match="pair cannot be empty"):
            TradingPair(
                exchange="binance",
                pair="",  # Empty
                base_asset="BTC",
                quote_asset="USDT"
            )
    
    def test_trading_pair_empty_assets(self):
        """Test that empty assets raise ValueError."""
        with pytest.raises(ValueError, match="Asset cannot be empty"):
            TradingPair(
                exchange="binance",
                pair="BTCUSDT",
                base_asset="",  # Empty
                quote_asset="USDT"
            )
    
    def test_trading_pair_asset_normalization(self):
        """Test that assets are normalized to uppercase."""
        pair = TradingPair(
            exchange="binance",
            pair="BTCUSDT",
            base_asset="btc",  # lowercase
            quote_asset="usdt"  # lowercase
        )
        
        assert pair.base_asset == "BTC"
        assert pair.quote_asset == "USDT"


class TestSymbolsConfigEntry:
    """Test the SymbolsConfigEntry model."""
    
    def test_symbols_config_entry_creation(self):
        """Test creating a valid SymbolsConfigEntry."""
        entry = SymbolsConfigEntry(
            exchange="Binance",
            symbols=["BTCUSDT", "ETHUSDT"]
        )
        
        assert entry.exchange == "Binance"
        assert entry.market == "SPOT"  # Default value
        assert entry.symbols == ["BTCUSDT", "ETHUSDT"]
    
    def test_symbols_config_entry_invalid_market(self):
        """Test that invalid market raises ValueError."""
        with pytest.raises(ValueError, match="market must be 'SPOT'"):
            SymbolsConfigEntry(
                exchange="Binance",
                market="FUTURES",  # Invalid
                symbols=["BTCUSDT"]
            )
    
    def test_symbols_config_entry_duplicate_symbols(self):
        """Test that duplicate symbols are removed and sorted."""
        entry = SymbolsConfigEntry(
            exchange="Binance",
            symbols=["BTCUSDT", "ETHUSDT", "BTCUSDT", "ADAUSDT"]  # Duplicate BTCUSDT
        )
        
        # Should be sorted and deduplicated
        assert entry.symbols == ["ADAUSDT", "BTCUSDT", "ETHUSDT"]


class TestSymbolsConfig:
    """Test the SymbolsConfig model."""
    
    def test_symbols_config_from_trading_pairs(self):
        """Test creating SymbolsConfig from TradingPair list."""
        trading_pairs = [
            TradingPair(exchange="Binance", pair="BTCUSDT", base_asset="BTC", quote_asset="USDT"),
            TradingPair(exchange="Binance", pair="ETHUSDT", base_asset="ETH", quote_asset="USDT"),
            TradingPair(exchange="Bybit", pair="BTCUSDT", base_asset="BTC", quote_asset="USDT"),
        ]
        
        config = SymbolsConfig.from_trading_pairs(trading_pairs)
        
        assert len(config.entries) == 2  # Binance and Bybit
        
        # Check Binance entry
        binance_entry = next(e for e in config.entries if e.exchange == "Binance")
        assert binance_entry.market == "SPOT"
        assert "BTCUSDT" in binance_entry.symbols
        assert "ETHUSDT" in binance_entry.symbols
        
        # Check Bybit entry
        bybit_entry = next(e for e in config.entries if e.exchange == "Bybit")
        assert bybit_entry.market == "SPOT"
        assert "BTCUSDT" in bybit_entry.symbols
    
    def test_symbols_config_get_exchange_symbols(self):
        """Test getting symbols for a specific exchange."""
        config = SymbolsConfig(entries=[
            SymbolsConfigEntry(exchange="Binance", symbols=["BTCUSDT", "ETHUSDT"]),
            SymbolsConfigEntry(exchange="Bybit", symbols=["ADAUSDT"])
        ])
        
        binance_symbols = config.get_exchange_symbols("Binance")
        assert binance_symbols == ["BTCUSDT", "ETHUSDT"]
        
        bybit_symbols = config.get_exchange_symbols("bybit")  # Case insensitive
        assert bybit_symbols == ["ADAUSDT"]
        
        nonexistent_symbols = config.get_exchange_symbols("Kraken")
        assert nonexistent_symbols == []
    
    def test_symbols_config_statistics(self):
        """Test statistics methods."""
        config = SymbolsConfig(entries=[
            SymbolsConfigEntry(exchange="Binance", symbols=["BTCUSDT", "ETHUSDT"]),
            SymbolsConfigEntry(exchange="Bybit", symbols=["BTCUSDT", "ADAUSDT"])  # BTCUSDT duplicated
        ])
        
        assert config.get_exchange_count() == 2
        assert config.get_total_symbols_count() == 3  # Unique symbols: BTCUSDT, ETHUSDT, ADAUSDT


class TestYAMLExporter:
    """Test the YAML exporter functionality."""
    
    def test_export_trading_pairs_to_yaml(self):
        """Test exporting trading pairs to YAML format."""
        trading_pairs = [
            TradingPair(exchange="Binance", pair="BTCUSDT", base_asset="BTC", quote_asset="USDT"),
            TradingPair(exchange="Binance", pair="ETHUSDT", base_asset="ETH", quote_asset="USDT"),
            TradingPair(exchange="Bybit", pair="BTCUSDT", base_asset="BTC", quote_asset="USDT"),
        ]
        
        exporter = YAMLExporter()
        yaml_content = exporter.export_trading_pairs(trading_pairs)
        
        # Parse the YAML to verify structure
        data = yaml.safe_load(yaml_content)
        
        assert isinstance(data, list)
        assert len(data) == 2  # Binance and Bybit
        
        # Check structure matches expected format
        for entry in data:
            assert 'exchange' in entry
            assert 'market' in entry
            assert 'symbols' in entry
            assert entry['market'] == 'SPOT'
            assert isinstance(entry['symbols'], list)
    
    def test_export_empty_trading_pairs(self):
        """Test exporting empty trading pairs list."""
        exporter = YAMLExporter()
        yaml_content = exporter.export_trading_pairs([])
        
        assert yaml_content == "[]"
    
    def test_yaml_format_validation(self):
        """Test YAML format validation."""
        exporter = YAMLExporter()
        
        # Valid YAML
        valid_yaml = """
- exchange: "Binance"
  market: "SPOT"
  symbols:
    - BTCUSDT
    - ETHUSDT
- exchange: "Bybit"
  market: "SPOT"
  symbols:
    - BTCUSDT
"""
        assert exporter.validate_yaml_format(valid_yaml) == True
        
        # Invalid YAML (missing required fields)
        invalid_yaml = """
- exchange: "Binance"
  symbols:
    - BTCUSDT
"""
        assert exporter.validate_yaml_format(invalid_yaml) == False
        
        # Invalid YAML (wrong structure)
        invalid_yaml2 = """
exchange: "Binance"
market: "SPOT"
symbols:
  - BTCUSDT
"""
        assert exporter.validate_yaml_format(invalid_yaml2) == False
    
    def test_round_trip_yaml_conversion(self):
        """Test that we can export to YAML and load it back correctly."""
        trading_pairs = [
            TradingPair(exchange="Binance", pair="BTCUSDT", base_asset="BTC", quote_asset="USDT"),
            TradingPair(exchange="Bybit", pair="ETHUSDT", base_asset="ETH", quote_asset="USDT"),
        ]
        
        exporter = YAMLExporter()
        
        # Export to YAML
        yaml_content = exporter.export_trading_pairs(trading_pairs)
        
        # Load back from YAML
        loaded_config = exporter.load_from_file_content(yaml_content)
        
        # Verify the loaded data
        assert loaded_config.get_exchange_count() == 2
        assert "BTCUSDT" in loaded_config.get_exchange_symbols("Binance")
        assert "ETHUSDT" in loaded_config.get_exchange_symbols("Bybit")
    
    def test_get_exchange_statistics(self):
        """Test getting statistics from trading pairs."""
        trading_pairs = [
            TradingPair(exchange="Binance", pair="BTCUSDT", base_asset="BTC", quote_asset="USDT"),
            TradingPair(exchange="Binance", pair="ETHUSDT", base_asset="ETH", quote_asset="USDT"),
            TradingPair(exchange="Bybit", pair="BTCUSDT", base_asset="BTC", quote_asset="USDT"),
        ]
        
        exporter = YAMLExporter()
        stats = exporter.get_exchange_statistics(trading_pairs)
        
        assert stats['total_pairs'] == 3
        assert stats['total_exchanges'] == 2
        
        # Check exchange-specific stats
        assert 'Binance' in stats['exchanges']
        assert 'Bybit' in stats['exchanges']
        
        binance_stats = stats['exchanges']['Binance']
        assert binance_stats['pair_count'] == 2
        assert 'BTC' in binance_stats['base_assets']
        assert 'ETH' in binance_stats['base_assets']
        assert 'USDT' in binance_stats['quote_assets']


class TestExchangeResponse:
    """Test the ExchangeResponse model."""
    
    def test_exchange_response_creation(self):
        """Test creating an ExchangeResponse."""
        trading_pairs = [
            TradingPair(exchange="Binance", pair="BTCUSDT", base_asset="BTC", quote_asset="USDT")
        ]
        
        response = ExchangeResponse(
            trading_pairs=trading_pairs,
            exchange_name="Binance"
        )
        
        assert response.exchange_name == "Binance"
        assert response.total_pairs_count == 1  # Auto-calculated
        assert response.success == True  # Default
        assert response.error_message is None
    
    def test_exchange_response_auto_count(self):
        """Test that total_pairs_count is automatically calculated."""
        trading_pairs = [
            TradingPair(exchange="Binance", pair="BTCUSDT", base_asset="BTC", quote_asset="USDT"),
            TradingPair(exchange="Binance", pair="ETHUSDT", base_asset="ETH", quote_asset="USDT")
        ]
        
        response = ExchangeResponse(
            trading_pairs=trading_pairs,
            exchange_name="Binance",
            total_pairs_count=999  # This should be overridden
        )
        
        assert response.total_pairs_count == 2  # Auto-calculated from trading_pairs length


class TestCreateSampleYAML:
    """Test the sample YAML creation function."""
    
    def test_create_sample_yaml(self):
        """Test creating sample YAML output."""
        yaml_content = create_sample_yaml()
        
        # Should be valid YAML
        data = yaml.safe_load(yaml_content)
        assert isinstance(data, list)
        assert len(data) > 0
        
        # Should contain expected exchanges
        exchange_names = [entry['exchange'] for entry in data]
        assert 'Binance' in exchange_names
        assert 'Bybit' in exchange_names
        assert 'OKX' in exchange_names


# Helper method for YAMLExporter to load from YAML content string
def load_from_file_content(yaml_exporter, yaml_content: str) -> SymbolsConfig:
    """Helper method to load SymbolsConfig from YAML content string."""
    yaml_data = yaml.safe_load(yaml_content)
    
    # Convert from YAML format to SymbolsConfig
    entries = []
    for entry_data in yaml_data:
        entry = SymbolsConfigEntry(
            exchange=entry_data['exchange'],
            market=entry_data.get('market', 'SPOT'),
            symbols=entry_data.get('symbols', [])
        )
        entries.append(entry)
    
    return SymbolsConfig(entries=entries)

# Add the helper method to YAMLExporter class
YAMLExporter.load_from_file_content = load_from_file_content


if __name__ == "__main__":
    # Run tests if executed directly
    pytest.main([__file__, "-v"]) 