use crate::{
    models::{NormalizedEvent, NormalizedTrade},
    streams::{ExchangeStreamError, Parser, mexc::model::MexcMessage},
};

#[derive(Debug, Clone, Default)]
pub struct MexcParser;

impl MexcParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser<Vec<NormalizedEvent>> for MexcParser {
    type Error = ExchangeStreamError;

    fn parse(&self, text: &str) -> Result<Option<Vec<NormalizedEvent>>, Self::Error> {
        let msg: MexcMessage = serde_json::from_str(text)
            .map_err(|e| ExchangeStreamError::Message(format!("Failed to parse JSON: {e}")))?;
        match msg {
            MexcMessage::Trade(trade_msg) => {
                let trades: Vec<NormalizedTrade> = trade_msg.try_into()?;
                Ok(Some(
                    trades.into_iter().map(NormalizedEvent::Trade).collect(),
                ))
            }
            MexcMessage::BookTicker(ticker_msg) => {
                let quote = ticker_msg.try_into()?;
                Ok(Some(vec![NormalizedEvent::Quote(quote)]))
            }
            MexcMessage::Ack(_) => Ok(None),
        }
    }
}
