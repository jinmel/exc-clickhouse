use serde::{Deserialize, Serialize};

use crate::{
    models::{NormalizedQuote, NormalizedTrade},
    streams::ExchangeStreamError,
    models::TradeSide,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TradeEvent {
    #[serde(rename = "e")]
    pub event_type: String,

    #[serde(rename = "E")]
    pub event_time: u64,

    #[serde(rename = "s")]
    pub symbol: String,

    #[serde(rename = "t")]
    pub trade_id: u64,

    #[serde(rename = "p")]
    pub price: String,

    #[serde(rename = "q")]
    pub quantity: String,

    #[serde(rename = "T")]
    pub trade_order_time: u64,

    #[serde(rename = "m")]
    pub is_buyer_maker: bool,

    #[serde(skip, rename = "M")]
    pub _ignore: bool,
}

impl TryInto<NormalizedTrade> for TradeEvent {
    type Error = ExchangeStreamError;

    fn try_into(self) -> Result<NormalizedTrade, Self::Error> {
        // Parse price and quantity strings to f64
        let price = self.price.parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid price value: {}", e)))?;

        let amount = self.quantity.parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid quantity value: {}", e)))?;

        Ok(NormalizedTrade::new(
            "binance",
            &self.symbol,
            self.event_time,
            if self.is_buyer_maker { TradeSide::Sell } else { TradeSide::Buy },
            price,
            amount,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BookTickerEvent {
    #[serde(rename = "u")]
    pub update_id: u64,

    #[serde(rename = "s")]
    pub symbol: String,

    #[serde(rename = "b")]
    pub best_bid: String,

    #[serde(rename = "B")]
    pub best_bid_quantity: String,

    #[serde(rename = "a")]
    pub best_ask: String,

    #[serde(rename = "A")]
    pub best_ask_quantity: String,
}

impl TryInto<NormalizedQuote> for BookTickerEvent {
    type Error = ExchangeStreamError;

    fn try_into(self) -> Result<NormalizedQuote, Self::Error> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| ExchangeStreamError::ParseError(format!("Failed to get timestamp: {}", e)))?
            .as_micros() as u64;

        // Parse ask and bid values
        let ask_amount = self.best_ask_quantity.parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid ask quantity value: {}", e)))?;

        let ask_price = self.best_ask.parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid ask price value: {}", e)))?;

        let bid_price = self.best_bid.parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid bid price value: {}", e)))?;

        let bid_amount = self.best_bid_quantity.parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid bid quantity value: {}", e)))?;

        Ok(NormalizedQuote::new(
            "binance",
            &self.symbol,
            timestamp,
            ask_amount,
            ask_price,
            bid_price,
            bid_amount,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
#[serde(untagged)]
pub enum Event {
    Trade(TradeEvent),
    Quote(BookTickerEvent),
}
