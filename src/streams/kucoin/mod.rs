use async_trait::async_trait;
use futures::stream::Stream;
use std::pin::Pin;
use uuid::Uuid;

use crate::streams::kucoin::parser::KucoinParser;
use crate::{
    models::NormalizedEvent,
    streams::{
        exchange_stream::ExchangeStreamBuilder, subscription::KucoinSubscription, ExchangeClient,
        ExchangeStreamError, StreamSymbols, StreamType, WebsocketStream,
    },
};

use tokio::time::Duration;

pub mod model;
pub mod parser;

#[derive(Clone)]

pub struct KucoinClient {
    base_url: String,
    subscription: KucoinSubscription,
    symbols: Vec<String>,
}

impl KucoinClient {
    pub fn builder() -> KucoinClientBuilder {
        KucoinClientBuilder::default()
    }
}

#[async_trait]
impl WebsocketStream for KucoinClient {
    type Error = ExchangeStreamError;
    type EventStream =
        Pin<Box<dyn Stream<Item = Result<NormalizedEvent, ExchangeStreamError>> + Send + 'static>>;

    async fn stream_events(&self) -> Result<Self::EventStream, Self::Error> {
        let parser = KucoinParser::new();
        let stream =
            ExchangeStreamBuilder::new(&self.base_url, None, parser, self.subscription.clone())
                .build();
        Ok(stream)
    }
}

impl ExchangeClient for KucoinClient {
    fn get_exchange_name(&self) -> &'static str {
        "kucoin"
    }

    fn get_symbols(&self) -> &[String] {
        &self.symbols
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

    pub async fn build(self) -> eyre::Result<KucoinClient> {
        #[derive(serde::Deserialize)]
        struct BulletResp {
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
            .first()
            .ok_or_else(|| eyre::eyre!("no instance server"))?;
        let connect_id = Uuid::new_v4();
        let mut url = url::Url::parse(&server.endpoint)
            .map_err(|e| eyre::eyre!("Failed to parse URL: {}", e))?;
        url.query_pairs_mut()
            .append_pair("token", &resp.data.token)
            .append_pair("connectId", &connect_id.to_string());
        let base_url = url.to_string();
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

        Ok(KucoinClient {
            base_url,
            subscription,
            symbols: self.symbols,
        })
    }
}
