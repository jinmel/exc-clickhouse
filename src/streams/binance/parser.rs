use crate::{
    models::NormalizedEvent,
    streams::{ExchangeStreamError, Parser, binance::model::Response},
};

#[derive(Debug, Clone)]
pub struct BinanceParser {}

impl BinanceParser {
    pub fn new() -> Self {
        Self {}
    }
}

impl Parser<NormalizedEvent> for BinanceParser {
    type Error = ExchangeStreamError;

    fn parse(&self, text: &str) -> Result<Option<NormalizedEvent>, Self::Error> {
        let value: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| ExchangeStreamError::MessageError(format!("Failed to parse JSON: {e}")))?;

        // Extract data field from the JSON
        let data = value.get("data").ok_or_else(|| {
            ExchangeStreamError::MessageError("Missing 'data' field in JSON".to_string())
        })?;

        let event = serde_json::from_value::<Response>(data.clone()).map_err(|e| {
            ExchangeStreamError::MessageError(format!("Failed to parse NormalizedEvent: {e}"))
        })?;

        let normalized = match event {
            Response::Trade(trade) => Some(NormalizedEvent::Trade(trade.try_into()?)),
            Response::Quote(quote) => Some(NormalizedEvent::Quote(quote.try_into()?)),
            Response::Subscription(result) => {
                tracing::debug!("Subscription result: {result:?}");
                None
            }
        };
        Ok(normalized)
    }
}
