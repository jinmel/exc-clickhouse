use async_trait::async_trait;

use crate::streams::binancefutures::parser::BinanceFuturesParser;
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

/// Default WebSocket URL for Binance Futures
pub const DEFAULT_BINANCE_FUTURES_WS_URL: &str = "wss://fstream.binance.com/stream";
#[allow(unused)]
pub const TESTNET_BINANCE_FUTURES_WS_URL: &str = "wss://stream.binancefuture.com/stream";

pub mod model;
pub mod parser;

#[derive(Clone)]
pub struct BinanceFuturesClient {
    base_url: String,
    subscription: BinanceSubscription,
    symbols: Vec<String>,
}

impl BinanceFuturesClient {
    /// Creates a new BinanceFuturesClientBuilder with default values
    pub fn builder() -> BinanceFuturesClientBuilder {
        BinanceFuturesClientBuilder::default()
    }
}

#[async_trait]
impl WebsocketStream for BinanceFuturesClient {
    type Error = ExchangeStreamError;
    type EventStream =
        Pin<Box<dyn Stream<Item = Result<NormalizedEvent, ExchangeStreamError>> + Send + 'static>>;

    async fn stream_events(&self) -> Result<Self::EventStream, Self::Error> {
        tracing::debug!("Binance Futures URL: {}", self.base_url);
        let timeout = Duration::from_secs(23 * 60 * 60); // Binance has 24 hour timeout.
        let parser = BinanceFuturesParser::new();
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

impl ExchangeClient for BinanceFuturesClient {
    fn get_exchange_name(&self) -> &'static str {
        "binance-futures"
    }

    fn get_symbols(&self) -> &[String] {
        &self.symbols
    }
}

/// Builder for the BinanceFuturesClient struct
pub struct BinanceFuturesClientBuilder {
    symbols: Vec<String>,
    base_url: String,
}

impl Default for BinanceFuturesClientBuilder {
    fn default() -> Self {
        Self {
            symbols: vec![],
            base_url: DEFAULT_BINANCE_FUTURES_WS_URL.to_string(),
        }
    }
}

impl BinanceFuturesClientBuilder {
    pub fn add_symbols(mut self, symbols: Vec<impl Into<String>>) -> Self {
        self.symbols
            .extend(symbols.into_iter().map(|s| s.into().to_lowercase()));
        self
    }

    /// Build the BinanceFuturesClient instance
    pub fn build(self) -> eyre::Result<BinanceFuturesClient> {
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

        Ok(BinanceFuturesClient {
            subscription,
            base_url: self.base_url,
            symbols: self.symbols,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binance_futures_client_builder() {
        let client = BinanceFuturesClient::builder()
            .add_symbols(vec!["BTCUSDT", "ETHUSDT"])
            .build()
            .expect("Failed to build BinanceFuturesClient");

        assert_eq!(client.get_exchange_name(), "binancefutures");
        assert_eq!(client.get_symbols(), &["btcusdt", "ethusdt"]);
        assert_eq!(client.base_url, DEFAULT_BINANCE_FUTURES_WS_URL);
    }

    #[test]
    fn test_binance_futures_client_empty_symbols() {
        let client = BinanceFuturesClient::builder()
            .build()
            .expect("Failed to build BinanceFuturesClient with empty symbols");

        assert_eq!(client.get_exchange_name(), "binancefutures");
        assert!(client.get_symbols().is_empty());
    }
}