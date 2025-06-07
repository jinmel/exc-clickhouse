use async_trait::async_trait;

use crate::streams::kraken::parser::KrakenParser;
use crate::{
    models::NormalizedEvent,
    streams::{
        ExchangeStreamError, StreamSymbols, StreamType, WebsocketStream,
        exchange_stream::ExchangeStream, subscription::KrakenSubscription,
    },
};

pub const DEFAULT_KRAKEN_WS_URL: &str = "wss://ws.kraken.com";

pub mod model;
pub mod parser;

pub struct KrakenClient {
    base_url: String,
    subscription: KrakenSubscription,
}

impl KrakenClient {
    pub fn builder() -> KrakenClientBuilder {
        KrakenClientBuilder::default()
    }
}

#[async_trait]
impl WebsocketStream for KrakenClient {
    type Error = ExchangeStreamError;
    type EventStream = ExchangeStream<NormalizedEvent, KrakenParser, KrakenSubscription>;

    async fn stream_events(&self) -> Result<Self::EventStream, Self::Error> {
        tracing::debug!("Kraken URL: {}", self.base_url);
        let parser = KrakenParser::new();
        let mut stream =
            ExchangeStream::new(&self.base_url, None, parser, self.subscription.clone()).await?;
        let res = stream.run().await;
        if res.is_err() {
            tracing::error!("Error running exchange stream: {:?}", res.err());
        }
        Ok(stream)
    }
}

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

    pub fn build(self) -> eyre::Result<KrakenClient> {
        let mut subscription = KrakenSubscription::new();
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
        Ok(KrakenClient {
            base_url: self.base_url,
            subscription,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    #[ignore]
    async fn test_kraken_stream_event() {
        let client = KrakenClient::builder()
            .add_symbols(vec!["XBT/USD"])
            .build()
            .unwrap();
        let mut stream = client.stream_events().await.unwrap();
        let result = timeout(Duration::from_secs(10), stream.next()).await;
        assert!(result.is_ok(), "timed out waiting for event");
        let item = result.unwrap();
        assert!(item.is_some(), "no event received");
        assert!(item.unwrap().is_ok(), "event returned error");
    }
}
