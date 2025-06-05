use serde::Deserialize;

use crate::{
    models::{ExchangeName, NormalizedQuote, NormalizedTrade, TradeSide},
    streams::ExchangeStreamError,
};

#[derive(Debug, Deserialize, Clone)]
pub struct WsResponse<T> {
    pub topic: String,
    #[serde(rename = "type")]
    pub msg_type: String,
    pub data: Vec<T>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TradeEvent {
    #[serde(rename = "T")]
    pub timestamp: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "S")]
    pub side: String,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "v")]
    pub size: String,
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
        let side = match self.side.as_str() {
            "Buy" => TradeSide::Buy,
            "Sell" => TradeSide::Sell,
            other => {
                return Err(ExchangeStreamError::ParseError(format!(
                    "Unknown side: {other}",
                )))
            }
        };
        Ok(NormalizedTrade::new(
            ExchangeName::Bybit,
            &self.symbol,
            self.timestamp * 1000,
            side,
            price,
            amount,
        ))
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct TickerEvent {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "bid1Price")]
    pub bid_price: String,
    #[serde(rename = "bid1Size")]
    pub bid_size: String,
    #[serde(rename = "ask1Price")]
    pub ask_price: String,
    #[serde(rename = "ask1Size")]
    pub ask_size: String,
    #[serde(rename = "ts")]
    pub timestamp: u64,
}

impl TryInto<NormalizedQuote> for TickerEvent {
    type Error = ExchangeStreamError;

    fn try_into(self) -> Result<NormalizedQuote, Self::Error> {
        let ask_price = self
            .ask_price
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid ask price: {e}")))?;
        let ask_amount = self
            .ask_size
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid ask size: {e}")))?;
        let bid_price = self
            .bid_price
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid bid price: {e}")))?;
        let bid_amount = self
            .bid_size
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid bid size: {e}")))?;
        Ok(NormalizedQuote::new(
            ExchangeName::Bybit,
            &self.symbol,
            self.timestamp * 1000,
            ask_amount,
            ask_price,
            bid_price,
            bid_amount,
        ))
    }
}

