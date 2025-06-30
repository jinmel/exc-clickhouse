use async_trait::async_trait;

use crate::streams::hyperliquid::parser::HyperliquidParser;
use crate::{
    models::NormalizedEvent,
    streams::{
        exchange_stream::ExchangeStreamBuilder, subscription::HyperliquidSubscription, ExchangeClient,
        ExchangeStreamError, StreamSymbols, StreamType, WebsocketStream,
    },
};
use futures::stream::Stream;
use std::pin::Pin;
use tokio::time::Duration;

/// Default WebSocket URL for Hyperliquid mainnet
pub const DEFAULT_HYPERLIQUID_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";

pub mod model;
pub mod parser;

#[derive(Clone)]
pub struct HyperliquidClient {
    base_url: String,
    subscription: HyperliquidSubscription,
    symbols: Vec<String>,
}

impl HyperliquidClient {
    /// Creates a new HyperliquidClientBuilder with default values
    pub fn builder() -> HyperliquidClientBuilder {
        HyperliquidClientBuilder::default()
    }
}

#[async_trait]
impl WebsocketStream for HyperliquidClient {
    type Error = ExchangeStreamError;
    type EventStream =
        Pin<Box<dyn Stream<Item = Result<NormalizedEvent, ExchangeStreamError>> + Send + 'static>>;

    async fn stream_events(&self, cancellation_token: tokio_util::sync::CancellationToken) -> Result<Self::EventStream, Self::Error> {
        tracing::debug!("Hyperliquid URL: {} (with cancellation token)", self.base_url);
        let timeout = Duration::from_secs(30); // Keep connection alive with heartbeat
        let parser = HyperliquidParser::new();
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

impl ExchangeClient for HyperliquidClient {
    fn get_exchange_name(&self) -> &'static str {
        "hyperliquid"
    }

    fn get_symbols(&self) -> &[String] {
        &self.symbols
    }
}

/// Builder for the HyperliquidClient struct
pub struct HyperliquidClientBuilder {
    symbols: Vec<String>,
    base_url: String,
}

impl Default for HyperliquidClientBuilder {
    fn default() -> Self {
        Self {
            symbols: vec![],
            base_url: DEFAULT_HYPERLIQUID_WS_URL.to_string(),
        }
    }
}

impl HyperliquidClientBuilder {
    /// Add symbols to stream. Hyperliquid uses coin names like "BTC", "ETH", "SOL"
    pub fn add_symbols(mut self, symbols: Vec<impl Into<String>>) -> Self {
        self.symbols
            .extend(symbols.into_iter().map(|s| s.into().to_uppercase()));
        self
    }

    /// Build the HyperliquidClient instance
    pub fn build(self) -> eyre::Result<HyperliquidClient> {
        let mut subscription = HyperliquidSubscription::new();
        
        // Add both trades and l2Book subscriptions for each symbol
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

        Ok(HyperliquidClient {
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
    fn test_builder_creation() {
        let client = HyperliquidClient::builder()
            .add_symbols(vec!["BTC", "ETH", "SOL"])
            .build()
            .expect("Failed to build client");

        assert_eq!(client.get_exchange_name(), "hyperliquid");
        assert_eq!(client.get_symbols(), &["BTC", "ETH", "SOL"]);
        assert_eq!(client.base_url, DEFAULT_HYPERLIQUID_WS_URL);
    }
} 