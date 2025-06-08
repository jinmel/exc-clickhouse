use crate::{
    models::NormalizedEvent,
    streams::{
        ExchangeStreamError, Parser,
        kraken::model::{KrakenMessage, Response},
    },
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

impl Parser<Vec<NormalizedEvent>> for KrakenParser {
    type Error = ExchangeStreamError;

    fn parse(&self, text: &str) -> Result<Option<Vec<NormalizedEvent>>, Self::Error> {
        let message: KrakenMessage = serde_json::from_str(text)
            .map_err(|e| ExchangeStreamError::Message(format!("Failed to parse JSON: {e}")))?;

        match message {
            KrakenMessage::Response(response) => match response {
                Response::Trade(trade_msg) => {
                    let normalized_trades = trade_msg
                        .data
                        .iter()
                        .map(|trade_data| {
                            let normalized_trade = trade_data.clone().try_into()?;
                            Ok(NormalizedEvent::Trade(normalized_trade))
                        })
                        .collect::<Result<Vec<NormalizedEvent>, Self::Error>>()?;
                    Ok(Some(normalized_trades))
                }
                Response::Ticker(ticker_msg) => {
                    // Process the first ticker in the data array
                    let normalized_quotes = ticker_msg
                        .data
                        .iter()
                        .map(|ticker_data| {
                            let normalized_quote = ticker_data.clone().try_into()?;
                            Ok(NormalizedEvent::Quote(normalized_quote))
                        })
                        .collect::<Result<Vec<NormalizedEvent>, Self::Error>>()?;
                    Ok(Some(normalized_quotes))
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
                        return Err(ExchangeStreamError::Subscription(format!(
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
