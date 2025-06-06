pub mod binance;
pub mod bybit;
pub mod exchange_stream;
pub mod subscription;
// pub mod upbit; // TODO: Needs refactoring to use new builder pattern

use crate::models::{NormalizedEvent, NormalizedTrade, NormalizedQuote};
use async_trait::async_trait;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use tokio::time::Duration;
use std::pin::Pin;

// Re-export commonly used types
// pub use exchange_stream::{ExchangeStream, ExchangeStreamBuilder};

#[async_trait]
pub trait WebsocketStream {
    type Error: std::error::Error + Send + Sync + 'static;
    type EventStream: Stream<Item = Result<NormalizedEvent, Self::Error>> + Send + Unpin + 'static;

    async fn stream_events(&self) -> Result<Self::EventStream, Self::Error>;
}

#[async_trait]
pub trait ExchangeClient {
    async fn normalized_trades(&self) -> Result<Pin<Box<dyn Stream<Item = Result<NormalizedTrade, ExchangeStreamError>> + Send>>, ExchangeStreamError>;
    async fn normalized_quotes(&self) -> Result<Pin<Box<dyn Stream<Item = Result<NormalizedQuote, ExchangeStreamError>> + Send>>, ExchangeStreamError>;
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
    ParseError(String),
    #[error("Message error: {0}")]
    MessageError(String),
    #[error("Connection error: {0}")]
    ConnectionError(String),
    #[error("Subscription error: {0}")]
    SubscriptionError(String),
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),
}
