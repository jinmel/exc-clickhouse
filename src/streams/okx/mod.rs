use async_trait::async_trait;

use crate::streams::okx::parser::OkxParser;
use crate::{
    models::NormalizedEvent,
    streams::{
        ExchangeStreamError, StreamSymbols, StreamType, WebsocketStream,
        exchange_stream::ExchangeStream, subscription::OkxSubscription,
    },
};

pub const DEFAULT_OKX_WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";

pub mod model;
pub mod parser;

pub struct OkxClient {
    base_url: String,
    subscription: OkxSubscription,
}

impl OkxClient {
    pub fn builder() -> OkxClientBuilder {
        OkxClientBuilder::default()
    }
}

#[async_trait]
impl WebsocketStream for OkxClient {
    type Error = ExchangeStreamError;
    type EventStream = ExchangeStream<NormalizedEvent, OkxParser, OkxSubscription>;

    async fn stream_events(&self) -> Result<Self::EventStream, Self::Error> {
        tracing::debug!("Okx URL: {}", self.base_url);
        let parser = OkxParser::new();
        let mut stream =
            ExchangeStream::new(&self.base_url, None, parser, self.subscription.clone()).await?;
        let res = stream.run().await;
        if res.is_err() {
            tracing::error!("Error running exchange stream: {:?}", res.err());
        }
        Ok(stream)
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
    async fn test_okx_stream_event() {
        let client = OkxClient::builder()
            .add_symbols(vec!["BTC-USDT"])
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
