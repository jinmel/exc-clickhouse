use async_trait::async_trait;
use uuid::Uuid;

use crate::streams::kucoin::parser::KucoinParser;
use crate::{
    models::NormalizedEvent,
    streams::{
        exchange_stream::ExchangeStream,
        subscription::KucoinSubscription,
        ExchangeStreamError, StreamSymbols, StreamType, WebsocketStream,
    },
};

use tokio::time::Duration;

pub mod model;
pub mod parser;

pub struct KucoinClient {
    base_url: String,
    subscription: KucoinSubscription,
}

impl KucoinClient {
    pub fn builder() -> KucoinClientBuilder {
        KucoinClientBuilder::default()
    }
}

#[async_trait]
impl WebsocketStream for KucoinClient {
    type Error = ExchangeStreamError;
    type EventStream = ExchangeStream<NormalizedEvent, KucoinParser, KucoinSubscription>;

    async fn stream_events(&self) -> Result<Self::EventStream, Self::Error> {
        let parser = KucoinParser::new();
        let mut stream = ExchangeStream::new(&self.base_url, None, parser, self.subscription.clone()).await?;
        let res = stream.run().await;
        if res.is_err() {
            tracing::error!("Error running exchange stream: {:?}", res.err());
        }
        Ok(stream)
    }
}

pub struct KucoinClientBuilder {
    symbols: Vec<String>,
    rest_endpoint: String,
}

impl Default for KucoinClientBuilder {
    fn default() -> Self {
        Self {
            symbols: vec![],
            rest_endpoint: "https://api.kucoin.com".to_string(),
        }
    }
}

impl KucoinClientBuilder {
    pub fn add_symbols(mut self, symbols: Vec<impl Into<String>>) -> Self {
        self.symbols.extend(symbols.into_iter().map(|s| s.into()));
        self
    }

    pub fn with_rest_endpoint(mut self, ep: impl Into<String>) -> Self {
        self.rest_endpoint = ep.into();
        self
    }

    pub async fn build(self) -> eyre::Result<KucoinClient> {
        #[derive(serde::Deserialize)]
        struct BulletResp {
            code: String,
            data: BulletData,
        }
        #[derive(serde::Deserialize)]
        struct BulletData {
            token: String,
            #[serde(rename = "instanceServers")]
            instance_servers: Vec<InstanceServer>,
        }
        #[derive(serde::Deserialize)]
        struct InstanceServer {
            endpoint: String,
            #[serde(rename = "pingInterval")]
            ping_interval: u64,
            #[serde(rename = "pingTimeout")]
            ping_timeout: u64,
        }

        let client = reqwest::Client::new();
        let resp: BulletResp = client
            .post(format!("{}/api/v1/bullet-public", self.rest_endpoint))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let server = resp
            .data
            .instance_servers
            .get(0)
            .ok_or_else(|| eyre::eyre!("no instance server"))?;
        let connect_id = Uuid::new_v4();
        let base_url = format!(
            "{}?token={}&connectId={}",
            server.endpoint.trim_end_matches('/'),
            resp.data.token,
            connect_id
        );

        let mut subscription = KucoinSubscription::new(Duration::from_millis(server.ping_interval));
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

        Ok(KucoinClient { base_url, subscription })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    #[ignore]
    async fn test_kucoin_stream_event() {
        let client = KucoinClient::builder()
            .add_symbols(vec![
                "BTC-USDT", "ETH-USDT", "XRP-USDT", "BCH-USDT", "ADA-USDT",
                "DOGE-USDT", "SOL-USDT", "DOT-USDT", "TRX-USDT", "LTC-USDT",
            ])
            .build()
            .await
            .unwrap();
        let mut stream = client.stream_events().await.unwrap();

        for _ in 0..50 {
            let result = timeout(Duration::from_secs(10), stream.next()).await;
            assert!(result.is_ok(), "timed out waiting for event");
            let item = result.unwrap();
            assert!(item.is_some(), "no event received");
            assert!(item.unwrap().is_ok(), "event returned error");
        }
    }
}
