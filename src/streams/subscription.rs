use super::{StreamEndpoint, StreamType, Subscription};
use serde::Serialize;
use tokio::time::Duration;

#[derive(Debug, Clone)]
pub struct BinanceSubscription {
    endpoints: Vec<StreamEndpoint>,
}

impl BinanceSubscription {
    pub fn new() -> Self {
        Self { endpoints: vec![] }
    }

    pub fn add_markets(&mut self, markets: Vec<StreamEndpoint>) {
        self.endpoints.extend(markets);
    }

    pub fn to_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        #[derive(Serialize)]
        struct SubscriptionMessage {
            method: String,
            params: Vec<String>,
            id: Option<u64>,
        }

        let pararms = self
            .endpoints
            .iter()
            .map(|market| {
                let stream_type = match market.stream_type {
                    StreamType::Trade => "trade",
                    StreamType::Quote => "bookTicker",
                };
                format!(
                    "{symbol}@{stream_type}",
                    symbol = market.symbol.to_lowercase(),
                    stream_type = stream_type
                )
            })
            .collect::<Vec<String>>();
        let id = rand::random::<u64>();
        let subscription_message = SubscriptionMessage {
            method: "SUBSCRIBE".to_string(),
            params: pararms,
            id: Some(id),
        };
        serde_json::to_value(subscription_message)
    }
}

impl Subscription for BinanceSubscription {
    fn messages(&self) -> Vec<tokio_tungstenite::tungstenite::Message> {
        let subscription_message = self.to_json().unwrap();
        vec![tokio_tungstenite::tungstenite::Message::Text(
            subscription_message.to_string().into(),
        )]
    }

    fn heartbeat(&self) -> Option<tokio_tungstenite::tungstenite::Message> {
        None
    }

    fn heartbeat_interval(&self) -> Duration {
        Duration::from_secs(30)
    }
}

struct BybitSubscription {
    endpoints: Vec<StreamEndpoint>,
}

impl BybitSubscription {
    pub fn new() -> Self {
        Self { endpoints: vec![] }
    }

    pub fn add_market(&mut self, market: StreamEndpoint) {
        self.endpoints.push(market);
    }

    pub fn add_markets(&mut self, markets: Vec<StreamEndpoint>) {
        self.endpoints.extend(markets);
    }

    pub fn get_subscription_message(&self) -> Result<serde_json::Value, serde_json::Error> {
        #[derive(Serialize)]
        struct SubscriptionMessage {
            req_id: Option<String>,
            op: &'static str,
            args: Vec<String>,
        }

        let args = self
            .endpoints
            .iter()
            .map(|market| {
                let stream_type = match market.stream_type {
                    StreamType::Trade => "publicTrade",
                    StreamType::Quote => "orderbook.200",
                };
                format!(
                    "{stream_type}.{symbol}",
                    symbol = market.symbol,
                    stream_type = stream_type
                )
            })
            .collect::<Vec<String>>();

        let subscription_message = SubscriptionMessage {
            req_id: None,
            op: "subscribe",
            args,
        };
        serde_json::to_value(subscription_message)
    }
}

impl Subscription for BybitSubscription {
    fn messages(&self) -> Vec<tokio_tungstenite::tungstenite::Message> {
        let subscription_message = self.get_subscription_message().unwrap();
        vec![tokio_tungstenite::tungstenite::Message::Text(
            subscription_message.to_string().into(),
        )]
    }

    fn heartbeat(&self) -> Option<tokio_tungstenite::tungstenite::Message> {
        #[derive(Serialize)]
        struct Ping {
            req_id: Option<String>,
            op: &'static str,
        }

        let ping = Ping {
            req_id: None,
            op: "ping",
        };

        let ping_message = serde_json::to_value(ping).unwrap();
        Some(tokio_tungstenite::tungstenite::Message::Text(
            ping_message.to_string().into(),
        ))
    }

    fn heartbeat_interval(&self) -> Duration {
        Duration::from_secs(20)
    }
}
