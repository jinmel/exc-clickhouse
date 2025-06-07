use async_trait::async_trait;

use crate::streams::coinbase::parser::CoinbaseParser;
use crate::{
    models::NormalizedEvent,
    streams::{
        ExchangeStreamError, StreamSymbols, StreamType, WebsocketStream,
        exchange_stream::ExchangeStream, subscription::CoinbaseSubscription,
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
    type EventStream = ExchangeStream<NormalizedEvent, CoinbaseParser, CoinbaseSubscription>;

    async fn stream_events(&self) -> Result<Self::EventStream, Self::Error> {
        tracing::debug!("Coinbase URL: {}", self.base_url);
        let parser = CoinbaseParser::new();
        let mut stream =
            ExchangeStream::new(&self.base_url, None, parser, self.subscription.clone()).await?;
        let res = stream.run().await;
        if res.is_err() {
            tracing::error!("Error running exchange stream: {:?}", res.err());
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    #[ignore]
    async fn test_coinbase_stream_event() {
        let symbols = vec![
            "BTC-USD",
            "ETH-USD",
            "SOL-USD",
            "ADA-USD",
            "DOGE-USD",
            "AVAX-USD",
            "XRP-USD",
            "LTC-USD",
            "LINK-USD",
            "MATIC-USD",
        ];
        let client = CoinbaseClient::builder()
            .add_symbols(symbols)
            .build()
            .unwrap();
        let mut stream = client.stream_events().await.unwrap();
        let count = timeout(Duration::from_secs(30), async {
            let mut received = 0;
            while let Some(event) = stream.next().await {
                if event.is_ok() {
                    received += 1;
                    if received >= 50 {
                        break;
                    }
                }
            }
            received
        })
        .await
        .expect("timed out waiting for events");
        assert!(count >= 50, "insufficient events: {}", count);
    }
}
