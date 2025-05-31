use arrayvec::ArrayString;
use serde::{Deserialize, Serialize};

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
    #[serde(rename = "upbit")]
    Upbit,
}

impl ToString for ExchangeName {
    fn to_string(&self) -> String {
        match self {
            ExchangeName::Binance => "binance".to_string(),
            ExchangeName::Bybit => "bybit".to_string(),
            ExchangeName::Okx => "okx".to_string(),
            ExchangeName::Coinbase => "coinbase".to_string(),
            ExchangeName::Kraken => "kraken".to_string(),
            ExchangeName::Upbit => "upbit".to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub enum TradeSide {
    Buy, 
    Sell
}

impl std::fmt::Display for TradeSide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TradeSide::Buy => write!(f, "buy"),
            TradeSide::Sell => write!(f, "sell"),
        }
    }
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

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedQuote {
    pub exchange: ExchangeName,
    pub symbol: ArrayString<20>,
    pub timestamp: u64,
    pub ask_amount: f64,
    pub ask_price: f64,
    pub bid_price: f64,
    pub bid_amount: f64,
}

impl NormalizedTrade {
    pub fn new(
        exchange: ExchangeName,
        symbol: &str,
        timestamp: u64,
        side: TradeSide,
        price: f64,
        amount: f64,
    ) -> Self {
        let symbol = ArrayString::from(symbol).expect("Symbol is too long");
        
        Self {
            exchange,
            symbol,
            timestamp,
            side,
            price,
            amount,
        }
    }
}

impl NormalizedQuote {
    pub fn new(
        exchange: ExchangeName,
        symbol: &str,
        timestamp: u64,
        ask_amount: f64,
        ask_price: f64,
        bid_price: f64,
        bid_amount: f64,
    ) -> Self {
        let symbol = ArrayString::from(symbol).expect("Symbol is too long");

        Self {
            exchange,
            symbol,
            timestamp,
            ask_amount,
            ask_price,
            bid_price,
            bid_amount,
        }
    }
}
