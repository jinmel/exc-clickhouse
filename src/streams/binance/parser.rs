use crate::{
    models::NormalizedEvent,
    streams::{binance::model::BinanceMessage, ExchangeStreamError, Parser},
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
            .map_err(|e| ExchangeStreamError::Message(format!("Failed to parse JSON: {e}")))?;

        let value = value.get("data").unwrap_or(&value);

        let event = serde_json::from_value::<BinanceMessage>(value.to_owned()).map_err(|e| {
            ExchangeStreamError::Message(format!("Failed to parse NormalizedEvent: {e}"))
        })?;

        let normalized = match event {
            BinanceMessage::Trade(trade) => Some(vec![NormalizedEvent::Trade(trade.try_into()?)]),
            BinanceMessage::Quote(quote) => Some(vec![NormalizedEvent::Quote(quote.try_into()?)]),
            BinanceMessage::Subscription(result) => {
                if result.result.is_some() {
                    return Err(ExchangeStreamError::Subscription(format!(
                        "Subscription result: {result:?}"
                    )));
                }
                None
            }
            BinanceMessage::Error(error) => {
                return Err(ExchangeStreamError::Message(format!(
                    "Binance error: code={}, msg={}",
                    error.error.code, error.error.msg
                )));
            }
        };
        Ok(normalized)
    }
}
