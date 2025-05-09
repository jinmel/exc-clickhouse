use serde::{Deserialize, Serialize};
use arrayvec::ArrayString;

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub enum ExchangeName {
    #[serde(rename = "binance")]
    Binance,
    #[serde(rename = "bybit")]
    Bybit,
    #[serde(rename = "okx")]
    Okx,
    #[serde(rename = "coinbase")]
    Coinbase,
    #[serde(rename = "kraken")]
    Kraken,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub enum TradeSide {
    Buy, 
    Sell
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub enum NormalizedEvent {
    Trade(NormalizedTrade),
    Quote(NormalizedQuote),
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedTrade {
    pub exchange: ExchangeName,
    pub symbol: ArrayString<20>,
    pub timestamp: u64,
    pub side: TradeSide,
    pub price: f64,
    pub amount: f64,
}

impl TradeSide {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            TradeSide::Buy => b"buy",
            TradeSide::Sell => b"sell",
        }
    }
}
#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedQuote {
    pub exchange: [u8; 20],
    pub symbol: [u8; 20],
    pub timestamp: u64,
    pub ask_amount: f64,
    pub ask_price: f64,
    pub bid_price: f64,
    pub bid_amount: f64,
}

impl NormalizedTrade {
    pub fn new(
        exchange: &str,
        symbol: &str,
        timestamp: u64,
        side: TradeSide,
        price: f64,
        amount: f64,
    ) -> Self {
        let mut exchange_bytes = [0u8; 20];
        let mut symbol_bytes = [0u8; 20];
        exchange_bytes[..exchange.len().min(20)].copy_from_slice(exchange.as_bytes());
        symbol_bytes[..symbol.len().min(20)].copy_from_slice(symbol.as_bytes());
        let mut side_bytes = [0u8; 5];
        side_bytes[..side.as_bytes().len()].copy_from_slice(side.as_bytes());

        Self {
            exchange: exchange_bytes,
            symbol: symbol_bytes,
            timestamp,
            side: side_bytes,
            price,
            amount,
        }
    }
}

impl NormalizedQuote {
    pub fn new(
        exchange: &str,
        symbol: &str,
        timestamp: u64,
        ask_amount: f64,
        ask_price: f64,
        bid_price: f64,
        bid_amount: f64,
    ) -> Self {
        let mut exchange_bytes = [0u8; 20];
        let mut symbol_bytes = [0u8; 20];
        exchange_bytes[..exchange.len().min(20)].copy_from_slice(exchange.as_bytes());
        symbol_bytes[..symbol.len().min(20)].copy_from_slice(symbol.as_bytes());

        Self {
            exchange: exchange_bytes,
            symbol: symbol_bytes,
            timestamp,
            ask_amount,
            ask_price,
            bid_price,
            bid_amount,
        }
    }
}
