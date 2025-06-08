use serde::{Deserialize, Serialize};

use crate::{
    models::{ExchangeName, NormalizedQuote, NormalizedTrade, TradeSide},
    streams::ExchangeStreamError,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Arg {
    pub channel: String,
    #[serde(rename = "instId")]
    pub inst_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Trade {
    #[serde(rename = "instId")]
    pub inst_id: String,
    #[serde(rename = "tradeId")]
    pub trade_id: String,
    pub px: String,
    pub sz: String,
    pub side: String,
    pub ts: String,
    #[serde(default)]
    pub count: Option<String>,
}

impl TryInto<NormalizedTrade> for Trade {
    type Error = ExchangeStreamError;

    fn try_into(self) -> Result<NormalizedTrade, Self::Error> {
        let price = self
            .px
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid price value: {e}")))?;
        let amount = self
            .sz
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid quantity value: {e}")))?;
        let ts = self
            .ts
            .parse::<u64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid timestamp: {e}")))?;
        let side = match self.side.as_str() {
            "buy" => TradeSide::Buy,
            "sell" => TradeSide::Sell,
            _ => {
                return Err(ExchangeStreamError::Message(format!(
                    "Unknown trade side: {}",
                    self.side
                )));
            }
        };
        Ok(NormalizedTrade::new(
            ExchangeName::Okx,
            &self.inst_id,
            ts * 1000,
            side,
            price,
            amount,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TradesMessage {
    pub arg: Arg,
    pub data: Vec<Trade>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Ticker {
    #[serde(rename = "instType")]
    pub inst_type: Option<String>,
    #[serde(rename = "instId")]
    pub inst_id: String,
    pub last: String,
    #[serde(rename = "lastSz")]
    pub last_sz: String,
    #[serde(rename = "askPx")]
    pub ask_px: String,
    #[serde(rename = "askSz")]
    pub ask_sz: String,
    #[serde(rename = "bidPx")]
    pub bid_px: String,
    #[serde(rename = "bidSz")]
    pub bid_sz: String,
    pub ts: String,
}

impl TryInto<NormalizedQuote> for Ticker {
    type Error = ExchangeStreamError;

    fn try_into(self) -> Result<NormalizedQuote, Self::Error> {
        let ask_amount = self
            .ask_sz
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid ask size: {e}")))?;
        let ask_price = self
            .ask_px
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid ask price: {e}")))?;
        let bid_price = self
            .bid_px
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid bid price: {e}")))?;
        let bid_amount = self
            .bid_sz
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid bid size: {e}")))?;
        let ts = self
            .ts
            .parse::<u64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid timestamp: {e}")))?;
        Ok(NormalizedQuote::new(
            ExchangeName::Okx,
            &self.inst_id,
            ts * 1000,
            ask_amount,
            ask_price,
            bid_price,
            bid_amount,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TickersMessage {
    pub arg: Arg,
    pub data: Vec<Ticker>,
}
