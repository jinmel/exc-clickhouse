use serde::{Deserialize, Serialize};

use crate::{
    models::{ExchangeName, NormalizedQuote, NormalizedTrade, TradeSide},
    streams::ExchangeStreamError,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TradeData {
    pub sequence: Option<String>,
    pub price: String,
    pub size: String,
    pub side: String,
    pub symbol: Option<String>,
    pub time: u64,
}

impl TryInto<NormalizedTrade> for TradeData {
    type Error = ExchangeStreamError;

    fn try_into(self) -> Result<NormalizedTrade, Self::Error> {
        let price = self
            .price
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid price value: {e}")))?;
        let size = self
            .size
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid size value: {e}")))?;
        let side = match self.side.as_str() {
            "buy" => TradeSide::Buy,
            "sell" => TradeSide::Sell,
            other => {
                return Err(ExchangeStreamError::MessageError(format!(
                    "Unknown side: {}",
                    other
                )));
            }
        };
        let symbol = self.symbol.unwrap_or_default();
        Ok(NormalizedTrade::new(
            ExchangeName::Kucoin,
            &symbol,
            self.time * 1000,
            side,
            price,
            size,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TickerData {
    pub best_ask: String,
    #[serde(rename = "bestAskSize")]
    pub best_ask_size: String,
    pub best_bid: String,
    #[serde(rename = "bestBidSize")]
    pub best_bid_size: String,
    pub time: u64,
    pub symbol: Option<String>,
}

impl TryInto<NormalizedQuote> for TickerData {
    type Error = ExchangeStreamError;

    fn try_into(self) -> Result<NormalizedQuote, Self::Error> {
        let ask_price = self
            .best_ask
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid ask price: {e}")))?;
        let ask_amount = self
            .best_ask_size
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid ask size: {e}")))?;
        let bid_price = self
            .best_bid
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid bid price: {e}")))?;
        let bid_amount = self
            .best_bid_size
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid bid size: {e}")))?;
        let symbol = self.symbol.unwrap_or_default();
        Ok(NormalizedQuote::new(
            ExchangeName::Kucoin,
            &symbol,
            self.time * 1000,
            ask_amount,
            ask_price,
            bid_price,
            bid_amount,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", untagged)]
pub enum KucoinMessage {
    Trade {
        topic: String,
        subject: String,
        data: TradeData,
        #[serde(rename = "type")]
        typ: String,
    },
    Ticker {
        topic: String,
        subject: String,
        data: TickerData,
        #[serde(rename = "type")]
        typ: String,
    },
    Other {
        #[serde(rename = "type")]
        typ: String,
    },
}
