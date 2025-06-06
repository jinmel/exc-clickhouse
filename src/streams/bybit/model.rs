use serde::{Deserialize, Serialize};

use crate::{
    models::{ExchangeName, NormalizedTrade, TradeSide},
    streams::ExchangeStreamError,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TradeData {
    #[serde(rename = "T")]
    pub trade_timestamp: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "S")]
    pub side: String,
    #[serde(rename = "v")]
    pub size: String,
    #[serde(rename = "p")]
    pub price: String,
}

impl TryInto<NormalizedTrade> for TradeData {
    type Error = ExchangeStreamError;

    fn try_into(self) -> Result<NormalizedTrade, Self::Error> {
        let price = self
            .price
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid price value: {e}")))?;
        let amount = self.size.parse::<f64>().map_err(|e| {
            ExchangeStreamError::MessageError(format!("Invalid quantity value: {e}"))
        })?;
        let side = match self.side.as_str() {
            "Buy" => TradeSide::Buy,
            "Sell" => TradeSide::Sell,
            _ => {
                return Err(ExchangeStreamError::MessageError(format!(
                    "Unknown trade side: {}",
                    self.side
                )));
            }
        };
        Ok(NormalizedTrade::new(
            ExchangeName::Bybit,
            &self.symbol,
            self.trade_timestamp * 1000,
            side,
            price,
            amount,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TradeMessage {
    pub topic: String,
    #[serde(rename = "type")]
    pub typ: String,
    pub ts: u64,
    pub data: Vec<TradeData>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OrderbookData {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(default, rename = "b")]
    pub bids: Vec<[String; 2]>,
    #[serde(default, rename = "a")]
    pub asks: Vec<[String; 2]>,
    pub u: Option<u64>,
    pub seq: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OrderbookMessage {
    pub topic: String,
    #[serde(rename = "type")]
    pub typ: String,
    pub ts: u64,
    #[serde(default)]
    pub cts: Option<u64>,
    pub data: OrderbookData,
}
