use serde::{Deserialize, Serialize};

use crate::{
    models::{ExchangeName, NormalizedQuote, NormalizedTrade, TradeSide},
    streams::ExchangeStreamError,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WsArg {
    pub channel: String,
    #[serde(rename = "instId")]
    pub inst_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WsResponse<T> {
    pub arg: WsArg,
    pub data: Vec<T>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TradeEvent {
    #[serde(rename = "instId")]
    pub inst_id: String,
    pub trade_id: String,
    pub px: String,
    pub sz: String,
    pub side: String,
    pub ts: String,
}

impl TryInto<NormalizedTrade> for TradeEvent {
    type Error = ExchangeStreamError;

    fn try_into(self) -> Result<NormalizedTrade, Self::Error> {
        let price = self
            .px
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid price: {e}")))?;
        let amount = self
            .sz
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid size: {e}")))?;
        let ts = self
            .ts
            .parse::<u64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid timestamp: {e}")))?;
        let side = match self.side.as_str() {
            "buy" => TradeSide::Buy,
            "sell" => TradeSide::Sell,
            _ => {
                return Err(ExchangeStreamError::ParseError(format!(
                    "Invalid side: {}",
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
#[serde(rename_all = "camelCase")]
pub struct TickerEvent {
    #[serde(rename = "instId")]
    pub inst_id: String,
    pub ask_px: String,
    pub ask_sz: String,
    pub bid_px: String,
    pub bid_sz: String,
    pub ts: String,
}

impl TryInto<NormalizedQuote> for TickerEvent {
    type Error = ExchangeStreamError;

    fn try_into(self) -> Result<NormalizedQuote, Self::Error> {
        let ask_price = self
            .ask_px
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid ask price: {e}")))?;
        let ask_amount = self
            .ask_sz
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid ask size: {e}")))?;
        let bid_price = self
            .bid_px
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid bid price: {e}")))?;
        let bid_amount = self
            .bid_sz
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid bid size: {e}")))?;
        let ts = self
            .ts
            .parse::<u64>()
            .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid timestamp: {e}")))?;
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
