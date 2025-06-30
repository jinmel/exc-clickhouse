use async_trait::async_trait;

use crate::streams::binance::parser::BinanceParser;
use crate::{
    models::NormalizedEvent,
    streams::{
        ExchangeClient, ExchangeStreamError, StreamSymbols, StreamType, WebsocketStream,
        exchange_stream::ExchangeStreamBuilder, subscription::BinanceSubscription,
    },
};
use futures::stream::Stream;
use std::pin::Pin;
use tokio::time::Duration;

/// Default WebSocket URL for Binance
pub const DEFAULT_BINANCE_WS_URL: &str = "wss://stream.binance.com:9443/stream";
#[allow(unused)]
pub const MARKET_ONLY_BINANCE_WS_URL: &str = "wss://data-stream.binance.vision/stream";
#[allow(unused)]
pub const US_BINANCE_WS_URL: &str = "wss://stream.binance.us:9443";

pub mod model;
pub mod parser;

#[derive(Clone)]
pub struct BinanceClient {
    base_url: String,
    subscription: BinanceSubscription,
    symbols: Vec<String>,
}

impl BinanceClient {
    /// Creates a new BinanceBuilder with default values
    pub fn builder() -> BinanceClientBuilder {
        BinanceClientBuilder::default()
    }
}

#[async_trait]
impl WebsocketStream for BinanceClient {
    type Error = ExchangeStreamError;
    type EventStream =
        Pin<Box<dyn Stream<Item = Result<NormalizedEvent, ExchangeStreamError>> + Send + 'static>>;

    async fn stream_events(&self, cancellation_token: tokio_util::sync::CancellationToken) -> Result<Self::EventStream, Self::Error> {
        tracing::debug!("Binance URL: {} (with cancellation token)", self.base_url);
        let timeout = Duration::from_secs(23 * 60 * 60); // Binance has 24 hour timeout.
        let parser = BinanceParser::new();
        let stream = ExchangeStreamBuilder::new(
            &self.base_url,
            Some(timeout),
            parser,
            self.subscription.clone(),
        )
        .with_cancellation(cancellation_token)
        .build();
        Ok(stream)
    }
}

impl ExchangeClient for BinanceClient {
    fn get_exchange_name(&self) -> &'static str {
        "binance"
    }

    fn get_symbols(&self) -> &[String] {
        &self.symbols
    }
}

/// Builder for the Binance struct
pub struct BinanceClientBuilder {
    symbols: Vec<String>,
    base_url: String,
}

impl Default for BinanceClientBuilder {
    fn default() -> Self {
        Self {
            symbols: vec![],
            base_url: DEFAULT_BINANCE_WS_URL.to_string(),
        }
    }
}

impl BinanceClientBuilder {
    pub fn add_symbols(mut self, symbols: Vec<impl Into<String>>) -> Self {
        self.symbols
            .extend(symbols.into_iter().map(|s| s.into().to_lowercase()));
        self
    }

    /// Build the Binance instance
    pub fn build(self) -> eyre::Result<BinanceClient> {
        let mut subscription = BinanceSubscription::new();
        subscription.add_markets(
            self.symbols
                .iter()
                .map(|s| StreamSymbols {
                    symbol: s.clone(),
                    stream_type: StreamType::Trade,
                })
                .collect(),
        );
        subscription.add_markets(
            self.symbols
                .iter()
                .map(|s| StreamSymbols {
                    symbol: s.clone(),
                    stream_type: StreamType::Quote,
                })
                .collect(),
        );

        Ok(BinanceClient {
            subscription,
            base_url: self.base_url,
            symbols: self.symbols,
        })
    }
}
