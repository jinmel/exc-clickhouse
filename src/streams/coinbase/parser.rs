use crate::{
    models::{NormalizedEvent, NormalizedQuote, NormalizedTrade},
    streams::{ExchangeStreamError, Parser},
};

use super::model::{MatchEvent, TickerEvent};

#[derive(Debug, Clone)]
pub struct CoinbaseParser;

impl Default for CoinbaseParser {
    fn default() -> Self {
        Self::new()
    }
}

impl CoinbaseParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser<NormalizedEvent> for CoinbaseParser {
    type Error = ExchangeStreamError;

    fn parse(&self, text: &str) -> Result<Option<NormalizedEvent>, Self::Error> {
        let value: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| ExchangeStreamError::MessageError(format!("Failed to parse JSON: {e}")))?;
        let typ = value.get("type").and_then(|v| v.as_str()).unwrap_or("");
        match typ {
            "match" | "last_match" => {
                let msg: MatchEvent = serde_json::from_value(value).map_err(|e| {
                    ExchangeStreamError::MessageError(format!("Failed to parse match: {e}"))
                })?;
                let trade: NormalizedTrade = msg.try_into()?;
                Ok(Some(NormalizedEvent::Trade(trade)))
            }
            "ticker" => {
                let msg: TickerEvent = serde_json::from_value(value).map_err(|e| {
                    ExchangeStreamError::MessageError(format!("Failed to parse ticker: {e}"))
                })?;
                let quote: NormalizedQuote = msg.try_into()?;
                Ok(Some(NormalizedEvent::Quote(quote)))
            }
            "subscriptions" => Ok(None),
            _ => {
                tracing::warn!("Unknown event type: {} msg: {}", &typ, &text);
                Ok(None)
            }
        }
    }
}
