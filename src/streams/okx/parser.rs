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

impl Parser<Vec<NormalizedEvent>> for OkxParser {
    type Error = ExchangeStreamError;

    fn parse(&self, text: &str) -> Result<Option<Vec<NormalizedEvent>>, Self::Error> {
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
            let normalized_trades = msg
                .data
                .iter()
                .map(|trade| {
                    let normalized_trade = trade.clone().try_into()?;
                    Ok(NormalizedEvent::Trade(normalized_trade))
                })
                .collect::<Result<Vec<NormalizedEvent>, Self::Error>>()?;
            return Ok(Some(normalized_trades));
        } else if channel == "tickers" {
            let msg: TickersMessage = serde_json::from_value(value).map_err(|e| {
                ExchangeStreamError::MessageError(format!("Failed to parse ticker: {e}"))
            })?;
            let normalized_quotes = msg
                .data
                .iter()
                .map(|ticker| {
                    let normalized_quote = ticker.clone().try_into()?;
                    Ok(NormalizedEvent::Quote(normalized_quote))
                })
                .collect::<Result<Vec<NormalizedEvent>, Self::Error>>()?;
            return Ok(Some(normalized_quotes));
        }
        Ok(None)
    }
}
