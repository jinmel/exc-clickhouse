use crate::{
    models::{NormalizedEvent, NormalizedQuote},
    streams::{ExchangeStreamError, Parser},
};

use super::model::{KrakenMessage, TradeMessage, trade_item_to_normalized};
use std::convert::TryInto;

#[derive(Debug, Clone)]
pub struct KrakenParser;

impl KrakenParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser<NormalizedEvent> for KrakenParser {
    type Error = ExchangeStreamError;

    fn parse(&self, text: &str) -> Result<Option<NormalizedEvent>, Self::Error> {
        if text.starts_with('{') {
            // system or subscription message
            return Ok(None);
        }

        let msg: KrakenMessage = serde_json::from_str(text)
            .map_err(|e| ExchangeStreamError::MessageError(format!("Failed to parse JSON: {e}")))?;

        match msg {
            KrakenMessage::Trade(TradeMessage { pair, data }) => {
                if let Some(first) = data.first() {
                    let trade = trade_item_to_normalized(first, &pair)?;
                    Ok(Some(NormalizedEvent::Trade(trade)))
                } else {
                    Ok(None)
                }
            }
            KrakenMessage::Ticker(ticker) => {
                let quote: NormalizedQuote = ticker.try_into()?;
                Ok(Some(NormalizedEvent::Quote(quote)))
            }
            KrakenMessage::Other => Ok(None),
        }
    }
}
