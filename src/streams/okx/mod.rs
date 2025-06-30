use async_trait::async_trait;
use futures::stream::Stream;
use std::pin::Pin;

use crate::streams::okx::parser::OkxParser;
use crate::{
    models::NormalizedEvent,
    streams::{
        ExchangeClient, ExchangeStreamError, StreamSymbols, StreamType, WebsocketStream,
        exchange_stream::ExchangeStreamBuilder, subscription::OkxSubscription,
    },
};

pub const DEFAULT_OKX_WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";

pub mod model;
pub mod parser;

#[derive(Clone)]
pub struct OkxClient {
    base_url: String,
    subscription: OkxSubscription,
    symbols: Vec<String>,
}

impl OkxClient {
    pub fn builder() -> OkxClientBuilder {
        OkxClientBuilder::default()
    }
}

#[async_trait]
impl WebsocketStream for OkxClient {
    type Error = ExchangeStreamError;
    type EventStream =
        Pin<Box<dyn Stream<Item = Result<NormalizedEvent, ExchangeStreamError>> + Send + 'static>>;

    async fn stream_events(&self) -> Result<Self::EventStream, Self::Error> {
        tracing::debug!("Okx URL: {}", self.base_url);
        let parser = OkxParser::new();
        let stream =
            ExchangeStreamBuilder::new(&self.base_url, None, parser, self.subscription.clone())
                .build();
        Ok(stream)
    }
}

impl ExchangeClient for OkxClient {
    fn get_exchange_name(&self) -> &'static str {
        "okx"
    }

    fn get_symbols(&self) -> &[String] {
        &self.symbols
    }
}

pub struct OkxClientBuilder {
    symbols: Vec<String>,
    base_url: String,
}

impl Default for OkxClientBuilder {
    fn default() -> Self {
        Self {
            symbols: vec![],
            base_url: DEFAULT_OKX_WS_URL.to_string(),
        }
    }
}

impl OkxClientBuilder {
    pub fn add_symbols(mut self, symbols: Vec<impl Into<String>>) -> Self {
        self.symbols.extend(symbols.into_iter().map(|s| s.into()));
        self
    }

    pub fn build(self) -> eyre::Result<OkxClient> {
        let mut subscription = OkxSubscription::new();
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

        Ok(OkxClient {
            subscription,
            base_url: self.base_url,
            symbols: self.symbols,
        })
    }
}
