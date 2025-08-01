pub mod binance;
pub mod bybit;
pub mod coinbase;
pub mod exchange_stream;
pub mod kraken;
pub mod kucoin;
pub mod okx;
pub mod subscription;
// pub mod upbit; // TODO: Needs refactoring to use new builder pattern

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

/// Trait for exchange clients to provide metadata about themselves
/// This allows for cleaner interfaces without passing exchange names as strings
pub trait ExchangeClient {
    /// Returns the name of the exchange (e.g., "binance", "coinbase")
    fn get_exchange_name(&self) -> &'static str;

    /// Returns the list of symbols this client is configured to stream
    fn get_symbols(&self) -> &[String];
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
    fn to_messages(
        &self,
    ) -> Result<Vec<tokio_tungstenite::tungstenite::Message>, serde_json::Error> {
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
    Stream(String),
    #[error("Message error: {0}")]
    Message(String),
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("Subscription error: {0}")]
    Subscription(String),
}
