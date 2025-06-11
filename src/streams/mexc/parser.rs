use crate::{
    models::NormalizedEvent,
    streams::{ExchangeStreamError, Parser, mexc::model::MexcMessage},
};

#[derive(Debug, Clone)]
pub struct MexcParser {}

impl Default for MexcParser {
    fn default() -> Self {
        Self::new()
    }
}

impl MexcParser {
    pub fn new() -> Self {
        Self {}
    }
}

impl Parser<Vec<NormalizedEvent>> for MexcParser {
    type Error = ExchangeStreamError;

    fn parse(&self, text: &str) -> Result<Option<Vec<NormalizedEvent>>, Self::Error> {
        let event = serde_json::from_str::<MexcMessage>(text).map_err(|e| {
            ExchangeStreamError::Message(format!("Failed to parse MEXC message: {e}"))
        })?;

        let normalized = match event {
            MexcMessage::Trade(trade) => Some(vec![NormalizedEvent::Trade(trade.try_into()?)]),
            MexcMessage::Quote(quote) => Some(vec![NormalizedEvent::Quote(quote.try_into()?)]),
            MexcMessage::Subscription(result) => {
                if result.result.is_some() {
                    return Err(ExchangeStreamError::Subscription(format!(
                        "MEXC subscription result: {result:?}"
                    )));
                }
                None
            }
            MexcMessage::Error(error) => {
                return Err(ExchangeStreamError::Message(format!(
                    "MEXC error: code={}, msg={}",
                    error.code, error.msg
                )));
            }
        };
        Ok(normalized)
    }
}