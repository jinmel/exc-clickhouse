use crate::{
    models::NormalizedEvent,
    streams::{ExchangeStreamError, Parser, kraken::model::{KrakenMessage, Response}},
};

#[derive(Debug, Clone)]
pub struct KrakenParser {}

impl Default for KrakenParser {
    fn default() -> Self {
        Self::new()
    }
}

impl KrakenParser {
    pub fn new() -> Self {
        Self {}
    }
}

impl Parser<NormalizedEvent> for KrakenParser {
    type Error = ExchangeStreamError;

    fn parse(&self, text: &str) -> Result<Option<NormalizedEvent>, Self::Error> {
        let message: KrakenMessage = serde_json::from_str(text)
            .map_err(|e| ExchangeStreamError::MessageError(format!("Failed to parse JSON: {e}")))?;

        match message {
            KrakenMessage::Response(response) => match response {
                Response::Trade(trade_msg) => {
                    // Process the first trade in the data array
                    if let Some(trade_data) = trade_msg.data.first() {
                        let normalized_trade = trade_data.clone().try_into()?;
                        tracing::trace!(?normalized_trade, "Received trade message");
                        Ok(Some(NormalizedEvent::Trade(normalized_trade)))
                    } else {
                        Ok(None)
                    }
                }
                Response::Ticker(ticker_msg) => {
                    // Process the first ticker in the data array
                    if let Some(ticker_data) = ticker_msg.data.first() {
                        let normalized_quote = ticker_data.clone().try_into()?;
                        tracing::trace!(?normalized_quote, "Received ticker message");
                        Ok(Some(NormalizedEvent::Quote(normalized_quote)))
                    } else {
                        Ok(None)
                    }
                }
                Response::Status(_) => {
                    tracing::debug!("Received status message");
                    Ok(None)
                }
                Response::Heartbeat(_) => {
                    tracing::debug!("Received heartbeat message");
                    Ok(None)
                }
                Response::Unknown => {
                    tracing::warn!("Received unknown message type");
                    Ok(None)
                }
            },
            KrakenMessage::Pong(_) => {
                tracing::debug!("Received pong message");
                Ok(None)
            }
            KrakenMessage::Subscription(sub_result) => {
                if !sub_result.success {
                    if let Some(error) = sub_result.error {
                        return Err(ExchangeStreamError::SubscriptionError(format!(
                            "Subscription failed: {error}"
                        )));
                    }
                }
                tracing::debug!("Subscription result: {:?}", sub_result);
                Ok(None)
            }
        }
    }
}