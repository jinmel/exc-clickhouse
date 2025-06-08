use super::model::KucoinMessage;
use crate::{
    models::{NormalizedEvent, NormalizedQuote, NormalizedTrade},
    streams::{ExchangeStreamError, Parser},
};

#[derive(Debug, Clone)]
pub struct KucoinParser {}

impl Default for KucoinParser {
    fn default() -> Self {
        Self::new()
    }
}

impl KucoinParser {
    pub fn new() -> Self {
        Self {}
    }
}

impl Parser<Vec<NormalizedEvent>> for KucoinParser {
    type Error = ExchangeStreamError;

    fn parse(&self, text: &str) -> Result<Option<Vec<NormalizedEvent>>, Self::Error> {
        let value: KucoinMessage = serde_json::from_str(text)
            .map_err(|e| ExchangeStreamError::Message(format!("Failed to parse JSON: {e}")))?;

        match value {
            KucoinMessage::Trade(event) => {
                let trade: NormalizedTrade = event.try_into()?;
                Ok(Some(vec![NormalizedEvent::Trade(trade)]))
            }
            KucoinMessage::Ticker(event) => {
                let quote: NormalizedQuote = event.try_into()?;
                Ok(Some(vec![NormalizedEvent::Quote(quote)]))
            }
            KucoinMessage::Ack(event) => {
                tracing::debug!("received kucoin ack: {}", event.id);
                Ok(None)
            }
            KucoinMessage::Other { .. } => {
                tracing::warn!("received unhandled kucoin message: {}", text);
                Ok(None)
            }
        }
    }
}
