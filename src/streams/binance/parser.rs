use crate::{
    models::NormalizedEvent,
    streams::{ExchangeStreamError, Parser, binance::model::Response},
};

#[derive(Debug, Clone)]
pub struct BinanceParser {}

impl Default for BinanceParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BinanceParser {
    pub fn new() -> Self {
        Self {}
    }
}

impl Parser<Vec<NormalizedEvent>> for BinanceParser {
    type Error = ExchangeStreamError;

    fn parse(&self, text: &str) -> Result<Option<Vec<NormalizedEvent>>, Self::Error> {
        let value: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| ExchangeStreamError::MessageError(format!("Failed to parse JSON: {e}")))?;

        let value = value.get("data").unwrap_or(&value);

        let event = serde_json::from_value::<Response>(value.to_owned()).map_err(|e| {
            ExchangeStreamError::MessageError(format!("Failed to parse NormalizedEvent: {e}"))
        })?;

        let normalized = match event {
            Response::Trade(trade) => Some(vec![NormalizedEvent::Trade(trade.try_into()?)]),
            Response::Quote(quote) => Some(vec![NormalizedEvent::Quote(quote.try_into()?)]),
            Response::Subscription(result) => {
                if result.result.is_some() {
                    return Err(ExchangeStreamError::SubscriptionError(format!(
                        "Subscription result: {result:?}"
                    )));
                }
                None
            }
        };
        Ok(normalized)
    }
}
