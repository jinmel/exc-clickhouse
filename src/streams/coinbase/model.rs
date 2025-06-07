use chrono::DateTime;
use serde::{Deserialize, Serialize};

use crate::{
    models::{ExchangeName, NormalizedQuote, NormalizedTrade, TradeSide},
    streams::ExchangeStreamError,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MatchEvent {
    #[serde(rename = "trade_id")]
    pub trade_id: u64,
    pub maker_order_id: Option<String>,
    pub taker_order_id: Option<String>,
    pub side: String,
    pub size: String,
    pub price: String,
    pub product_id: String,
    pub sequence: Option<u64>,
    pub time: String,
}

impl TryInto<NormalizedTrade> for MatchEvent {
    type Error = ExchangeStreamError;

    fn try_into(self) -> Result<NormalizedTrade, Self::Error> {
        let price = self
            .price
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid price value: {e}")))?;
        let amount = self
            .size
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid size value: {e}")))?;
        let side = match self.side.to_lowercase().as_str() {
            "buy" => TradeSide::Buy,
            "sell" => TradeSide::Sell,
            _ => {
                return Err(ExchangeStreamError::MessageError(format!(
                    "Unknown trade side: {}",
                    self.side
                )));
            }
        };
        let ts = DateTime::parse_from_rfc3339(&self.time)
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid time: {e}")))?
            .timestamp_micros() as u64;
        Ok(NormalizedTrade::new(
            ExchangeName::Coinbase,
            &self.product_id,
            ts,
            side,
            price,
            amount,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TickerEvent {
    pub sequence: Option<u64>,
    pub product_id: String,
    pub price: String,
    pub best_bid: String,
    pub best_ask: String,
    pub best_bid_size: String,
    pub best_ask_size: String,
    pub time: String,
}

impl TryInto<NormalizedQuote> for TickerEvent {
    type Error = ExchangeStreamError;

    fn try_into(self) -> Result<NormalizedQuote, Self::Error> {
        let timestamp = DateTime::parse_from_rfc3339(&self.time)
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid time: {e}")))?
            .timestamp_micros() as u64;
        let bid_price = self
            .best_bid
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid bid price: {e}")))?;
        let bid_amount = self
            .best_bid_size
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid bid size: {e}")))?;
        let ask_price = self
            .best_ask
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid ask price: {e}")))?;
        let ask_amount = self
            .best_ask_size
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid ask size: {e}")))?;
        Ok(NormalizedQuote::new(
            ExchangeName::Coinbase,
            &self.product_id,
            timestamp,
            ask_amount,
            ask_price,
            bid_price,
            bid_amount,
        ))
    }
}
