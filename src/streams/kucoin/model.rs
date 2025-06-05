use serde::Deserialize;

use crate::{
    models::{ExchangeName, NormalizedQuote, NormalizedTrade, TradeSide},
    streams::ExchangeStreamError,
};

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TradeData {
    pub sequence: u64,
    pub price: String,
    pub size: String,
    pub side: String,
    pub symbol: String,
    pub trade_id: String,
    pub ts: u64,
}

impl TryInto<NormalizedTrade> for TradeData {
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
            "buy" => TradeSide::Buy,
            "sell" => TradeSide::Sell,
            other => {
                return Err(ExchangeStreamError::ParseError(format!(
                    "Unknown side: {other}"
                )))
            }
        };
        Ok(NormalizedTrade::new(
            ExchangeName::Kucoin,
            &self.symbol,
            self.ts,
            side,
            price,
            amount,
        ))
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TickerData {
    pub sequence: u64,
    pub best_bid_price: String,
    pub best_bid_size: String,
    pub best_ask_price: String,
    pub best_ask_size: String,
    pub ts: u64,
    pub symbol: String,
}

impl TryInto<NormalizedQuote> for TickerData {
    type Error = ExchangeStreamError;

    fn try_into(self) -> Result<NormalizedQuote, Self::Error> {
        let ask_price = self
            .best_ask_price
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid ask price: {e}")))?;
        let ask_amount = self
            .best_ask_size
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid ask size: {e}")))?;
        let bid_price = self
            .best_bid_price
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid bid price: {e}")))?;
        let bid_amount = self
            .best_bid_size
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid bid size: {e}")))?;
        Ok(NormalizedQuote::new(
            ExchangeName::Kucoin,
            &self.symbol,
            self.ts,
            ask_amount,
            ask_price,
            bid_price,
            bid_amount,
        ))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WsMessage<T> {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub subject: Option<String>,
    pub topic: String,
    pub data: T,
}
