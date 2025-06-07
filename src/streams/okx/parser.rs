use crate::{
    models::NormalizedEvent,
    streams::{
        ExchangeStreamError, Parser,
        okx::model::{TickersMessage, TradesMessage},
    },
};

#[derive(Debug, Clone)]
pub struct OkxParser {}

impl Default for OkxParser {
    fn default() -> Self {
        Self::new()
    }
}

impl OkxParser {
    pub fn new() -> Self {
        Self {}
    }
}

impl Parser<NormalizedEvent> for OkxParser {
    type Error = ExchangeStreamError;

    fn parse(&self, text: &str) -> Result<Option<NormalizedEvent>, Self::Error> {
        let value: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| ExchangeStreamError::MessageError(format!("Failed to parse JSON: {e}")))?;

        if value.get("event").is_some() {
            // subscription ack or error
            return Ok(None);
        }

        let channel = value
            .get("arg")
            .and_then(|v| v.get("channel"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if channel == "trades" || channel == "trades-all" {
            let msg: TradesMessage = serde_json::from_value(value).map_err(|e| {
                ExchangeStreamError::MessageError(format!("Failed to parse trade: {e}"))
            })?;
            if let Some(trade) = msg.data.into_iter().next() {
                let trade: crate::models::NormalizedTrade = trade.try_into()?;
                return Ok(Some(NormalizedEvent::Trade(trade)));
            }
            return Ok(None);
        } else if channel == "tickers" {
            let msg: TickersMessage = serde_json::from_value(value).map_err(|e| {
                ExchangeStreamError::MessageError(format!("Failed to parse ticker: {e}"))
            })?;
            if let Some(ticker) = msg.data.into_iter().next() {
                let quote: crate::models::NormalizedQuote = ticker.try_into()?;
                return Ok(Some(NormalizedEvent::Quote(quote)));
            }
            return Ok(None);
        }
        Ok(None)
    }
}
