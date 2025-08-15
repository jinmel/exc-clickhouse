"""
Core data models for trading pairs fetcher.

This module defines Pydantic models that exactly match the Rust structures:
- TradingPair: Individual trading pair with exchange info
- SymbolsConfigEntry: Exchange group with symbols list  
- SymbolsConfig: Root structure for YAML output
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, validator
from enum import Enum


class ExchangeName(str, Enum):
    """Supported exchange names."""
    BINANCE = "binance"
    BINANCE_FUTURES = "binance-futures"
    BYBIT = "bybit" 
    KRAKEN = "kraken"
    KUCOIN = "kucoin"
    COINBASE = "coinbase"
    OKX = "okx"


class TradingPair(BaseModel):
    """
    Trading pair model that exactly matches the Rust TradingPair struct.
    
    Fields match src/symbols.rs TradingPair:
    - exchange: String
    - trading_type: String ("SPOT" or "FUTURES")
    - pair: String (the symbol name like "BTCUSDT")
    - base_asset: String (like "BTC")
    - quote_asset: String (like "USDT")
    """
    exchange: str = Field(..., description="Exchange name (e.g., 'binance')")
    trading_type: str = Field(default="SPOT", description="Trading type: SPOT or FUTURES")
    pair: str = Field(..., description="Trading pair symbol (e.g., 'BTCUSDT')")
    base_asset: str = Field(..., description="Base asset (e.g., 'BTC')")
    quote_asset: str = Field(..., description="Quote asset (e.g., 'USDT')")
    
    @validator('trading_type')
    def validate_trading_type(cls, v):
        """Ensure trading_type is SPOT or FUTURES."""
        if v not in ["SPOT", "FUTURES"]:
            raise ValueError("trading_type must be 'SPOT' or 'FUTURES'")
        return v
    
    @validator('pair')
    def validate_pair_not_empty(cls, v):
        """Ensure pair is not empty."""
        if not v or not v.strip():
            raise ValueError("pair cannot be empty")
        return v.strip()
    
    @validator('base_asset', 'quote_asset')
    def validate_assets_not_empty(cls, v):
        """Ensure assets are not empty."""
        if not v or not v.strip():
            raise ValueError("Asset cannot be empty")
        return v.strip().upper()


class SymbolsConfigEntry(BaseModel):
    """
    Exchange configuration entry that matches Rust SymbolsConfigEntry struct.
    
    Used for YAML output in the format:
    - exchange: "Binance"
      market: "SPOT" 
      symbols:
        - BTCUSDT
        - ETHUSDT
    """
    exchange: str = Field(..., description="Exchange name")
    market: str = Field(default="SPOT", description="Market type: SPOT or FUTURES")
    symbols: List[str] = Field(default_factory=list, description="List of trading pair symbols")
    
    @validator('market')
    def validate_market_type(cls, v):
        """Ensure market is SPOT or FUTURES."""
        if v not in ["SPOT", "FUTURES"]:
            raise ValueError("market must be 'SPOT' or 'FUTURES'")
        return v
    
    @validator('symbols')
    def validate_symbols_unique(cls, v):
        """Remove duplicates and ensure symbols list is sorted."""
        return sorted(list(set(v)))  # Remove duplicates and sort


class SymbolsConfig(BaseModel):
    """
    Root configuration that matches Rust SymbolsConfig struct.
    
    Contains a list of SymbolsConfigEntry objects that will be serialized
    to YAML format compatible with the Rust application.
    """
    entries: List[SymbolsConfigEntry] = Field(default_factory=list, description="List of exchange configurations")
    
    @classmethod
    def from_trading_pairs(cls, trading_pairs: List[TradingPair]) -> "SymbolsConfig":
        """
        Create SymbolsConfig from a list of TradingPair objects.
        
        Groups trading pairs by exchange and trading type, creating separate entries
        for SPOT and FUTURES markets from the same exchange.
        """
        # Group pairs by exchange and trading type
        exchange_groups: Dict[str, Dict[str, List[str]]] = {}
        
        for pair in trading_pairs:
            if pair.exchange not in exchange_groups:
                exchange_groups[pair.exchange] = {}
            
            if pair.trading_type not in exchange_groups[pair.exchange]:
                exchange_groups[pair.exchange][pair.trading_type] = []
            
            exchange_groups[pair.exchange][pair.trading_type].append(pair.pair)
        
        # Create SymbolsConfigEntry for each exchange/market combination
        entries = []
        for exchange_name, trading_types in exchange_groups.items():
            for trading_type, symbols in trading_types.items():
                entry = SymbolsConfigEntry(
                    exchange=exchange_name,
                    market=trading_type,
                    symbols=sorted(list(set(symbols)))  # Remove duplicates and sort
                )
                entries.append(entry)
        
        # Sort entries by exchange name and then by market type for consistent output
        entries.sort(key=lambda x: (x.exchange, x.market))
        
        return cls(entries=entries)
    
    def get_exchange_symbols(self, exchange_name: str) -> List[str]:
        """Get symbols for a specific exchange."""
        for entry in self.entries:
            if entry.exchange.lower() == exchange_name.lower():
                return entry.symbols
        return []
    
    def get_total_symbols_count(self) -> int:
        """Get total number of unique symbols across all exchanges."""
        all_symbols = set()
        for entry in self.entries:
            all_symbols.update(entry.symbols)
        return len(all_symbols)
    
    def get_exchange_count(self) -> int:
        """Get number of exchanges."""
        return len(self.entries)


class ExchangeResponse(BaseModel):
    """
    Response wrapper for exchange API calls with metadata.
    
    Contains the fetched trading pairs along with metadata about the fetch operation.
    """
    trading_pairs: List[TradingPair] = Field(default_factory=list, description="List of trading pairs")
    exchange_name: str = Field(..., description="Name of the exchange")
    fetch_timestamp: datetime = Field(default_factory=datetime.utcnow, description="When the data was fetched")
    total_pairs_count: int = Field(default=0, description="Total number of pairs fetched")
    success: bool = Field(default=True, description="Whether the fetch was successful")
    error_message: Optional[str] = Field(default=None, description="Error message if fetch failed")
    
    @validator('total_pairs_count', always=True)
    def set_total_pairs_count(cls, v, values):
        """Automatically set total_pairs_count based on trading_pairs length."""
        trading_pairs = values.get('trading_pairs', [])
        return len(trading_pairs)


class FetchSummary(BaseModel):
    """
    Summary of a complete fetch operation across all exchanges.
    """
    total_pairs: int = Field(default=0, description="Total pairs across all exchanges")
    successful_exchanges: List[str] = Field(default_factory=list, description="Exchanges that were fetched successfully")
    failed_exchanges: List[str] = Field(default_factory=list, description="Exchanges that failed to fetch")
    fetch_timestamp: datetime = Field(default_factory=datetime.utcnow, description="When the fetch operation completed")
    processing_time_seconds: float = Field(default=0.0, description="Total processing time in seconds")
    per_exchange_statistics: Dict[str, Dict[str, Any]] = Field(default_factory=dict, description="Statistics per exchange") 