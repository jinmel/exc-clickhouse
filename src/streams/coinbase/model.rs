use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;

use crate::{
    models::{ExchangeName, NormalizedQuote, NormalizedTrade, TradeSide},
    streams::ExchangeStreamError,
};

#[derive(Debug, Deserialize, Clone)]
pub struct TradeEvent {
    pub trade_id: u64,
    pub side: String,
    pub price: String,
    pub size: String,
    pub product_id: String,
    pub time: String,
}

impl TryInto<NormalizedTrade> for TradeEvent {
    type Error = ExchangeStreamError;

    fn try_into(self) -> Result<NormalizedTrade, Self::Error> {
        let price = self
            .price
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid price: {e}")))?;
        let amount = self
            .size
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid size: {e}")))?;
        let ts: DateTime<Utc> = DateTime::parse_from_rfc3339(&self.time)
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid time: {e}")))?
            .with_timezone(&Utc);
        let side = match self.side.as_str() {
            "buy" => TradeSide::Buy,
            "sell" => TradeSide::Sell,
            other => {
                return Err(ExchangeStreamError::ParseError(format!(
                    "Unknown side: {other}"
                )));
            }
        };
        Ok(NormalizedTrade::new(
            ExchangeName::Coinbase,
            &self.product_id,
            ts.timestamp_micros() as u64,
            side,
            price,
            amount,
        ))
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct TickerEvent {
    pub product_id: String,
    pub best_bid: String,
    pub best_ask: String,
    #[serde(default)]
    pub best_bid_size: Option<String>,
    #[serde(default)]
    pub best_ask_size: Option<String>,
    pub time: String,
}

impl TryInto<NormalizedQuote> for TickerEvent {
    type Error = ExchangeStreamError;

    fn try_into(self) -> Result<NormalizedQuote, Self::Error> {
        let bid_price = self
            .best_bid
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid bid price: {e}")))?;
        let ask_price = self
            .best_ask
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid ask price: {e}")))?;
        let bid_amount = self
            .best_bid_size
            .as_deref()
            .unwrap_or("0")
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid bid size: {e}")))?;
        let ask_amount = self
            .best_ask_size
            .as_deref()
            .unwrap_or("0")
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid ask size: {e}")))?;
        let ts: DateTime<Utc> = DateTime::parse_from_rfc3339(&self.time)
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid time: {e}")))?
            .with_timezone(&Utc);
        Ok(NormalizedQuote::new(
            ExchangeName::Coinbase,
            &self.product_id,
            ts.timestamp_micros() as u64,
            ask_amount,
            ask_price,
            bid_price,
            bid_amount,
        ))
    }
}
