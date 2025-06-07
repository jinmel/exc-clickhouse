pub mod binance;
pub mod bybit;
pub mod kucoin;
pub mod exchange_stream;
pub mod subscription;

use crate::models::NormalizedEvent;
use async_trait::async_trait;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use tokio::time::Duration;

#[async_trait]
pub trait WebsocketStream {
    type Error: std::error::Error + Send + Sync + 'static;
    type EventStream: Stream<Item = Result<NormalizedEvent, Self::Error>> + Send + Unpin + 'static;

    async fn stream_events(&self) -> Result<Self::EventStream, Self::Error>;
}

pub trait Parser<T: Send + 'static> {
    type Error: std::error::Error + Send + Sync + 'static;

    fn parse(&self, text: &str) -> Result<Option<T>, Self::Error>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamType {
    Trade,
    Quote,
}

#[derive(Debug, Clone)]
pub struct StreamSymbols {
    pub stream_type: StreamType,
    pub symbol: String,
}

// Returns subscription message
pub trait Subscription {
    fn to_json(&self) -> Result<Vec<serde_json::Value>, serde_json::Error>;
    fn messages(&self) -> Result<Vec<tokio_tungstenite::tungstenite::Message>, serde_json::Error> {
        let subscription_messages = self.to_json()?;
        Ok(subscription_messages
            .iter()
            .map(|message| {
                tokio_tungstenite::tungstenite::Message::Text(message.to_string().into())
            })
            .collect())
    }
    fn heartbeat(&self) -> Option<tokio_tungstenite::tungstenite::Message>;
    fn heartbeat_interval(&self) -> Option<Duration>;
}

#[derive(Debug, thiserror::Error, Clone)]
#[non_exhaustive]
pub enum ExchangeStreamError {
    #[error("Stream error: {0}")]
    StreamError(String),
    #[error("Stream not connected: {0}")]
    StreamNotConnected(String),
    #[error("Parse error: {0}")]
    MessageError(String),
    #[error("Connection error: {0}")]
    ConnectionError(String),
    #[error("Subscription error: {0}")]
    SubscriptionError(String),
}
