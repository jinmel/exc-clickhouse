use super::{StreamSymbols, StreamType, Subscription};
use itertools::Itertools;
use serde::Serialize;
use tokio::time::Duration;

#[derive(Debug, Clone)]
pub struct BinanceSubscription {
    symbols: Vec<StreamSymbols>,
}

impl BinanceSubscription {
    pub fn new() -> Self {
        Self { symbols: vec![] }
    }

    pub fn add_markets(&mut self, symbols: Vec<StreamSymbols>) {
        self.symbols.extend(symbols);
    }
}

impl Subscription for BinanceSubscription {
    fn to_json(&self) -> Result<Vec<serde_json::Value>, serde_json::Error> {
        #[derive(Serialize)]
        struct SubscriptionMessage {
            method: String,
            params: Vec<String>,
            id: Option<String>,
        }

        let pararms = self
            .symbols
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
            id: Some(id.to_string()),
        };
        Ok(vec![serde_json::to_value(subscription_message)?])
    }

    fn heartbeat(&self) -> Option<tokio_tungstenite::tungstenite::Message> {
        None
    }

    fn heartbeat_interval(&self) -> Option<Duration> {
        None
    }
}

#[derive(Debug, Clone)]
pub struct BybitSubscription {
    symbols: Vec<StreamSymbols>,
}

impl BybitSubscription {
    pub fn new() -> Self {
        Self { symbols: vec![] }
    }

    pub fn add_markets(&mut self, markets: Vec<StreamSymbols>) {
        self.symbols.extend(markets);
    }
}

impl Subscription for BybitSubscription {
    fn to_json(&self) -> Result<Vec<serde_json::Value>, serde_json::Error> {
        #[derive(Serialize)]
        struct SubscriptionMessage {
            req_id: Option<String>,
            op: &'static str,
            args: Vec<String>,
        }

        let args = self
            .symbols
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

        let subscription_messages = args
            .iter()
            .chunks(5)
            .into_iter()
            .map(|args| {
                let subscription_message = SubscriptionMessage {
                    req_id: None,
                    op: "subscribe",
                    args: args.cloned().collect(),
                };
                let value = serde_json::to_value(subscription_message)?;
                Ok(value)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(subscription_messages)
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

    fn heartbeat_interval(&self) -> Option<Duration> {
        Some(Duration::from_secs(20))
    }
}

#[derive(Debug, Clone)]
pub struct KucoinSubscription {
    symbols: Vec<StreamSymbols>,
    ping_interval: Duration,
}

impl KucoinSubscription {
    pub fn new(ping_interval: Duration) -> Self {
        Self { symbols: vec![], ping_interval }
    }

    pub fn add_markets(&mut self, markets: Vec<StreamSymbols>) {
        self.symbols.extend(markets);
    }
}

impl Subscription for KucoinSubscription {
    fn to_json(&self) -> Result<Vec<serde_json::Value>, serde_json::Error> {
        #[derive(Serialize)]
        struct SubscriptionMessage {
            id: String,
            #[serde(rename = "type")]
            typ: &'static str,
            topic: String,
            #[serde(rename = "privateChannel")]
            private_channel: bool,
            response: bool,
        }

        let msgs = self
            .symbols
            .iter()
            .map(|m| {
                let topic_type = match m.stream_type {
                    StreamType::Trade => "/market/match",
                    StreamType::Quote => "/market/ticker",
                };
                let topic = format!("{}:{}", topic_type, m.symbol);
                let msg = SubscriptionMessage {
                    id: rand::random::<u64>().to_string(),
                    typ: "subscribe",
                    topic,
                    private_channel: false,
                    response: true,
                };
                serde_json::to_value(msg)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(msgs)
    }

    fn heartbeat(&self) -> Option<tokio_tungstenite::tungstenite::Message> {
        #[derive(Serialize)]
        struct PingMessage {
            id: String,
            #[serde(rename = "type")]
            typ: &'static str,
        }
        let msg = PingMessage {
            id: rand::random::<u64>().to_string(),
            typ: "ping",
        };
        Some(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&msg).unwrap().into(),
        ))
    }

    fn heartbeat_interval(&self) -> Option<Duration> {
        Some(self.ping_interval)
    }
}
