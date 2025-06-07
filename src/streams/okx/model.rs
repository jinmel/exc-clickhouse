use serde::{Deserialize, Serialize};

use crate::{
    models::{ExchangeName, NormalizedQuote, NormalizedTrade, TradeSide},
    streams::ExchangeStreamError,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Arg {
    pub channel: String,
    pub instId: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Trade {
    pub instId: String,
    pub tradeId: String,
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
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid price value: {e}")))?;
        let amount = self.sz.parse::<f64>().map_err(|e| {
            ExchangeStreamError::MessageError(format!("Invalid quantity value: {e}"))
        })?;
        let ts = self
            .ts
            .parse::<u64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid timestamp: {e}")))?;
        let side = match self.side.as_str() {
            "buy" => TradeSide::Buy,
            "sell" => TradeSide::Sell,
            _ => {
                return Err(ExchangeStreamError::MessageError(format!(
                    "Unknown trade side: {}",
                    self.side
                )));
            }
        };
        Ok(NormalizedTrade::new(
            ExchangeName::Okx,
            &self.instId,
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
    pub instType: Option<String>,
    pub instId: String,
    pub last: String,
    pub lastSz: String,
    pub askPx: String,
    pub askSz: String,
    pub bidPx: String,
    pub bidSz: String,
    pub ts: String,
}

impl TryInto<NormalizedQuote> for Ticker {
    type Error = ExchangeStreamError;

    fn try_into(self) -> Result<NormalizedQuote, Self::Error> {
        let ask_amount = self
            .askSz
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid ask size: {e}")))?;
        let ask_price = self
            .askPx
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid ask price: {e}")))?;
        let bid_price = self
            .bidPx
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid bid price: {e}")))?;
        let bid_amount = self
            .bidSz
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid bid size: {e}")))?;
        let ts = self
            .ts
            .parse::<u64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid timestamp: {e}")))?;
        Ok(NormalizedQuote::new(
            ExchangeName::Okx,
            &self.instId,
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
