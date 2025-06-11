"""
Trading Pairs Fetcher Package

A Python package for fetching SPOT trading pairs from multiple cryptocurrency exchanges.
Supports Binance, Bybit, OKX, Coinbase, KuCoin, and Kraken.

Example usage:
    from trading_pairs_fetcher import AdapterFactory, fetch_all_exchanges
    
    # Fetch from all exchanges
    responses = await fetch_all_exchanges()
    
    # Or fetch from specific exchanges
    factory = AdapterFactory()
    binance_response = await factory.fetch_from_exchange('binance')
"""

# Core models
try:
    from .models import (
        TradingPair,
        SymbolsConfigEntry,
        SymbolsConfig,
        ExchangeResponse,
        FetchSummary
    )
    from .yaml_exporter import YAMLExporter
    
    # Base adapter
    from .base_adapter import ExchangeAdapter
    
    # Individual exchange adapters
    from .binance_adapter import BinanceAdapter
    from .bybit_adapter import BybitAdapter
    from .okx_adapter import OKXAdapter
    from .coinbase_adapter import CoinbaseAdapter
    from .kucoin_adapter import KuCoinAdapter
    from .kraken_adapter import KrakenAdapter
    
    # Factory and convenience functions
    from .adapter_factory import (
        AdapterFactory,
        fetch_all_exchanges,
        fetch_single_exchange
    )

except ImportError:
    # Fallback for when running as standalone script
    from models import (
        TradingPair,
        SymbolsConfigEntry,
        SymbolsConfig,
        ExchangeResponse,
        FetchSummary
    )
    from yaml_exporter import YAMLExporter
    
    from base_adapter import ExchangeAdapter
    
    from binance_adapter import BinanceAdapter
    from bybit_adapter import BybitAdapter
    from okx_adapter import OKXAdapter
    from coinbase_adapter import CoinbaseAdapter
    from kucoin_adapter import KuCoinAdapter
    from kraken_adapter import KrakenAdapter
    
    from adapter_factory import (
        AdapterFactory,
        fetch_all_exchanges,
        fetch_single_exchange
    )


# Package metadata
__version__ = "1.0.0"
__author__ = "Trading Pairs Fetcher"
__description__ = "Multi-exchange SPOT trading pairs discovery service"

# Public API
__all__ = [
    # Models
    'TradingPair',
    'SymbolsConfigEntry', 
    'SymbolsConfig',
    'ExchangeResponse',
    'FetchSummary',
    
    # YAML exporter
    'YAMLExporter',
    
    # Base adapter
    'ExchangeAdapter',
    
    # Exchange adapters
    'BinanceAdapter',
    'BybitAdapter',
    'OKXAdapter',
    'CoinbaseAdapter',
    'KuCoinAdapter',
    'KrakenAdapter',
    
    # Factory and utilities
    'AdapterFactory',
    'fetch_all_exchanges',
    'fetch_single_exchange',
] 