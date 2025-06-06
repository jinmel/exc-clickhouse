use async_trait::async_trait;

use crate::streams::bybit::parser::BybitParser;
use crate::{
    models::NormalizedEvent,
    streams::{
        ExchangeStreamError, StreamEndpoint, StreamType, WebsocketStream,
        exchange_stream::ExchangeStream, subscription::BybitSubscription,
    },
};

pub const DEFAULT_BYBIT_WS_URL: &str = "wss://stream.bybit.com/v5/public/spot";

pub mod model;
pub mod parser;

pub struct BybitClient {
    base_url: String,
    subscription: BybitSubscription,
}

impl BybitClient {
    pub fn builder() -> BybitClientBuilder {
        BybitClientBuilder::default()
    }
}

#[async_trait]
impl WebsocketStream for BybitClient {
    type Error = ExchangeStreamError;
    type EventStream = ExchangeStream<NormalizedEvent, BybitParser, BybitSubscription>;

    async fn stream_events(&self) -> Result<Self::EventStream, Self::Error> {
        tracing::debug!("Bybit URL: {}", self.base_url);
        let parser = BybitParser::new();
        let mut stream = ExchangeStream::new(
            &self.base_url,
            None,
            parser,
            self.subscription.clone(),
        )
        .await?;
        let res = stream.run().await;
        if res.is_err() {
            tracing::error!("Error running exchange stream: {:?}", res.err());
        }
        Ok(stream)
    }
}

pub struct BybitClientBuilder {
    symbols: Vec<String>,
    base_url: String,
}

impl Default for BybitClientBuilder {
    fn default() -> Self {
        Self {
            symbols: vec![],
            base_url: DEFAULT_BYBIT_WS_URL.to_string(),
        }
    }
}

impl BybitClientBuilder {
    pub fn add_symbols(mut self, symbols: Vec<impl Into<String>>) -> Self {
        self.symbols.extend(symbols.into_iter().map(|s| s.into()));
        self
    }

    pub fn with_base_url(mut self, base_url: String) -> Self {
        self.base_url = base_url;
        self
    }

    pub fn build(self) -> eyre::Result<BybitClient> {
        let mut subscription = BybitSubscription::new();
        subscription.add_markets(
            self.symbols
                .iter()
                .map(|s| StreamEndpoint {
                    symbol: s.clone(),
                    stream_type: StreamType::Trade,
                })
                .collect(),
        );
        subscription.add_markets(
            self.symbols
                .iter()
                .map(|s| StreamEndpoint {
                    symbol: s.clone(),
                    stream_type: StreamType::Quote,
                })
                .collect(),
        );

        Ok(BybitClient {
            subscription,
            base_url: self.base_url,
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
    async fn test_bybit_stream_event() {
        let client = BybitClient::builder()
            .add_symbols(vec!["BTCUSDT"])
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
