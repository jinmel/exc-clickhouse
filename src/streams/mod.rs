pub mod binance;
pub mod bybit;
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
pub struct StreamEndpoint {
    pub stream_type: StreamType,
    pub symbol: String,
}

// Returns subscription message
pub trait Subscription {
    fn messages(&self) -> Vec<tokio_tungstenite::tungstenite::Message>;
    fn heartbeat(&self) -> Option<tokio_tungstenite::tungstenite::Message>;
    fn heartbeat_interval(&self) -> Duration;
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
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),
    #[error("Connection error: {0}")]
    ConnectionError(String),
    #[error("Subscription error: {0}")]
    SubscriptionError(String),
}

#[derive(Debug, thiserror::Error, Clone)]
#[non_exhaustive]
pub enum ParserError {
    #[error("Parse error: {0}")]
    ParseError(String),
}
