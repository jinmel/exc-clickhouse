use clickhouse::Row;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub enum NormalizedEvent {
    Trade(NormalizedTrade),
    Quote(NormalizedQuote),
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, Row)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedTrade {
    pub exchange: [u8; 20],
    pub symbol: [u8; 20],
    pub timestamp: u64,
    pub side: [u8; 5],
    pub price: f64,
    pub amount: f64,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
#[serde(rename_all = "camelCase")]
pub enum TradeSide {
    #[serde(rename = "buy")]
    Buy,
    #[serde(rename = "sell")]
    Sell,
}

impl TradeSide {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            TradeSide::Buy => b"buy",
            TradeSide::Sell => b"sell",
        }
    }
}
#[derive(Debug, Serialize, Deserialize, Copy, Clone, Row)]
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
