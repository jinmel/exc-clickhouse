use async_trait::async_trait;
use futures::stream::Stream;
use std::pin::Pin;
use crate::streams::kraken::parser::KrakenParser;

use crate::{
    models::NormalizedEvent,
    streams::{
        ExchangeStreamError, StreamSymbols, StreamType, WebsocketStream,
        exchange_stream::ExchangeStreamBuilder, subscription::KrakenSubscription,
    },
};

/// Default WebSocket URL for Kraken v2
pub const DEFAULT_KRAKEN_WS_URL: &str = "wss://ws.kraken.com/v2";

pub mod model;
pub mod parser;

pub struct KrakenClient {
    base_url: String,
    subscription: KrakenSubscription,
}

impl KrakenClient {
    /// Creates a new KrakenClientBuilder with default values
    pub fn builder() -> KrakenClientBuilder {
        KrakenClientBuilder::default()
    }
}

#[async_trait]
impl WebsocketStream for KrakenClient {
    type Error = ExchangeStreamError;
    type EventStream =
        Pin<Box<dyn Stream<Item = Result<NormalizedEvent, ExchangeStreamError>> + Send + 'static>>;

    async fn stream_events(&self) -> Result<Self::EventStream, Self::Error> {
        tracing::debug!("Kraken URL: {}", self.base_url);
        let parser = KrakenParser::new();
        let stream =
            ExchangeStreamBuilder::new(&self.base_url, None, parser, self.subscription.clone()).build();
        Ok(stream)
    }
}

/// Builder for the Kraken struct
pub struct KrakenClientBuilder {
    symbols: Vec<String>,
    base_url: String,
}

impl Default for KrakenClientBuilder {
    fn default() -> Self {
        Self {
            symbols: vec![],
            base_url: DEFAULT_KRAKEN_WS_URL.to_string(),
        }
    }
}

impl KrakenClientBuilder {
    pub fn add_symbols(mut self, symbols: Vec<impl Into<String>>) -> Self {
        self.symbols.extend(symbols.into_iter().map(|s| s.into()));
        self
    }

    /// Build the Kraken instance
    pub fn build(self) -> eyre::Result<KrakenClient> {
        let mut subscription = KrakenSubscription::new();

        // Add ticker subscriptions (quotes)
        subscription.add_markets(
            self.symbols
                .iter()
                .map(|s| StreamSymbols {
                    symbol: s.clone(),
                    stream_type: StreamType::Quote,
                })
                .collect(),
        );

        // Add trade subscriptions
        subscription.add_markets(
            self.symbols
                .iter()
                .map(|s| StreamSymbols {
                    symbol: s.clone(),
                    stream_type: StreamType::Trade,
                })
                .collect(),
        );

        Ok(KrakenClient {
            subscription,
            base_url: self.base_url,
        })
    }
}
