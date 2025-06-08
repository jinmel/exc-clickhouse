use async_trait::async_trait;
use futures::stream::Stream;
use std::pin::Pin;

use crate::streams::coinbase::parser::CoinbaseParser;
use crate::{
    models::NormalizedEvent,
    streams::{
        ExchangeStreamError, StreamSymbols, StreamType, WebsocketStream,
        exchange_stream::ExchangeStreamBuilder, subscription::CoinbaseSubscription,
    },
};

pub const DEFAULT_COINBASE_WS_URL: &str = "wss://ws-feed.exchange.coinbase.com";

pub mod model;
pub mod parser;

pub struct CoinbaseClient {
    base_url: String,
    subscription: CoinbaseSubscription,
}

impl CoinbaseClient {
    pub fn builder() -> CoinbaseClientBuilder {
        CoinbaseClientBuilder::default()
    }
}

#[async_trait]
impl WebsocketStream for CoinbaseClient {
    type Error = ExchangeStreamError;
    type EventStream =
        Pin<Box<dyn Stream<Item = Result<NormalizedEvent, ExchangeStreamError>> + Send + 'static>>;

    async fn stream_events(&self) -> Result<Self::EventStream, Self::Error> {
        tracing::debug!("Coinbase URL: {}", self.base_url);
        let parser = CoinbaseParser::new();
        let stream =
            ExchangeStreamBuilder::new(&self.base_url, None, parser, self.subscription.clone())
                .build();
        Ok(stream)
    }
}

pub struct CoinbaseClientBuilder {
    symbols: Vec<String>,
    base_url: String,
}

impl Default for CoinbaseClientBuilder {
    fn default() -> Self {
        Self {
            symbols: vec![],
            base_url: DEFAULT_COINBASE_WS_URL.to_string(),
        }
    }
}

impl CoinbaseClientBuilder {
    pub fn add_symbols(mut self, symbols: Vec<impl Into<String>>) -> Self {
        self.symbols
            .extend(symbols.into_iter().map(|s| s.into().to_uppercase()));
        self
    }

    pub fn build(self) -> eyre::Result<CoinbaseClient> {
        let mut subscription = CoinbaseSubscription::new();
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

        Ok(CoinbaseClient {
            subscription,
            base_url: self.base_url,
        })
    }
}
