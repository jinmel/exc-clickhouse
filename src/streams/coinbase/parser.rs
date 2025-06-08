use crate::{
    models::{NormalizedEvent, NormalizedQuote, NormalizedTrade},
    streams::{ExchangeStreamError, Parser},
};

use super::model::CoinbaseMessage;

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

impl Parser<Vec<NormalizedEvent>> for CoinbaseParser {
    type Error = ExchangeStreamError;

    fn parse(&self, text: &str) -> Result<Option<Vec<NormalizedEvent>>, Self::Error> {
        let message: CoinbaseMessage = serde_json::from_str(text)
            .map_err(|e| ExchangeStreamError::Message(format!("Failed to parse message: {e}")))?;
        match message {
            CoinbaseMessage::Match(event) => {
                let trade: NormalizedTrade = event.try_into()?;
                Ok(Some(vec![NormalizedEvent::Trade(trade)]))
            }
            CoinbaseMessage::Ticker(event) => {
                let quote: NormalizedQuote = event.try_into()?;
                Ok(Some(vec![NormalizedEvent::Quote(quote)]))
            }
            CoinbaseMessage::Subscriptions(_) => Ok(None),
            CoinbaseMessage::Error(_) => {
                tracing::warn!("Received error message: {}", text);
                Ok(None)
            }
        }
    }
}
