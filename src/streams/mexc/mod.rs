use async_trait::async_trait;

use crate::streams::mexc::parser::MexcParser;
use crate::{
    models::NormalizedEvent,
    streams::{
        ExchangeClient, ExchangeStreamError, StreamSymbols, StreamType, WebsocketStream,
        exchange_stream::ExchangeStreamBuilder, subscription::MexcSubscription,
    },
};
use futures::stream::Stream;
use std::pin::Pin;
use tokio::time::Duration;

/// Default WebSocket URL for MEXC
pub const DEFAULT_MEXC_WS_URL: &str = "wss://wbs.mexc.com/ws";

pub mod model;
pub mod parser;

pub struct MexcClient {
    base_url: String,
    subscription: MexcSubscription,
    symbols: Vec<String>,
}

impl MexcClient {
    /// Creates a new MexcBuilder with default values
    pub fn builder() -> MexcClientBuilder {
        MexcClientBuilder::default()
    }
}

#[async_trait]
impl WebsocketStream for MexcClient {
    type Error = ExchangeStreamError;
    type EventStream =
        Pin<Box<dyn Stream<Item = Result<NormalizedEvent, ExchangeStreamError>> + Send + 'static>>;

    async fn stream_events(&self) -> Result<Self::EventStream, Self::Error> {
        tracing::debug!("MEXC URL: {}", self.base_url);
        let timeout = Duration::from_secs(23 * 60 * 60); // MEXC has 24 hour timeout similar to Binance
        let parser = MexcParser::new();
        let stream = ExchangeStreamBuilder::new(
            &self.base_url,
            Some(timeout),
            parser,
            self.subscription.clone(),
        )
        .build();
        Ok(stream)
    }
}

impl ExchangeClient for MexcClient {
    fn get_exchange_name(&self) -> &'static str {
        "mexc"
    }

    fn get_symbols(&self) -> &[String] {
        &self.symbols
    }
}

/// Builder for the Mexc struct
pub struct MexcClientBuilder {
    symbols: Vec<String>,
    base_url: String,
}

impl Default for MexcClientBuilder {
    fn default() -> Self {
        Self {
            symbols: vec![],
            base_url: DEFAULT_MEXC_WS_URL.to_string(),
        }
    }
}

impl MexcClientBuilder {
    pub fn add_symbols(mut self, symbols: Vec<impl Into<String>>) -> Self {
        self.symbols
            .extend(symbols.into_iter().map(|s| s.into().to_uppercase()));
        self
    }

    /// Build the Mexc instance
    pub fn build(self) -> eyre::Result<MexcClient> {
        let mut subscription = MexcSubscription::new();
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

        Ok(MexcClient {
            subscription,
            base_url: self.base_url,
            symbols: self.symbols,
        })
    }
}