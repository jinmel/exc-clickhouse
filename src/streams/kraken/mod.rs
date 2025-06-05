use async_trait::async_trait;
use futures::SinkExt;
use tokio_tungstenite::tungstenite::Message;

use crate::{
    models::NormalizedEvent,
    streams::{CombinedStream, ExchangeStream, ExchangeStreamError},
};

pub const DEFAULT_KRAKEN_WS_URL: &str = "wss://ws.kraken.com/v1";

pub mod parser;

pub struct KrakenClient {
    symbols: Vec<String>,
    base_url: String,
    enable_quote: bool,
    enable_trade: bool,
}

impl KrakenClient {
    pub fn builder() -> KrakenClientBuilder {
        KrakenClientBuilder::default()
    }

    fn build_subscribe_messages(&self) -> Result<Vec<serde_json::Value>, ExchangeStreamError> {
        if self.symbols.is_empty() {
            return Err(ExchangeStreamError::InvalidConfiguration(
                "No symbols provided".to_string(),
            ));
        }

        let mut out = Vec::new();
        if self.enable_trade {
            out.push(serde_json::json!({
                "event": "subscribe",
                "pair": self.symbols,
                "subscription": {"name": "trade"}
            }));
        }
        if self.enable_quote {
            out.push(serde_json::json!({
                "event": "subscribe",
                "pair": self.symbols,
                "subscription": {"name": "ticker"}
            }));
        }
        Ok(out)
    }
}

#[async_trait]
impl CombinedStream for KrakenClient {
    type CombinedStream = ExchangeStream<NormalizedEvent>;

    async fn combined_stream(&self) -> Result<Self::CombinedStream, ExchangeStreamError> {
        let subscribe_msgs = self.build_subscribe_messages()?;
        ExchangeStream::new(
            &self.base_url,
            parser::parse_kraken_combined,
            None,
            Some(Box::new(move |mut ws| {
                Box::pin(async move {
                    for msg in subscribe_msgs {
                        ws.send(Message::Text(serde_json::to_string(&msg).unwrap().into()))
                            .await
                            .map_err(|e| ExchangeStreamError::StreamError(e.to_string()))?;
                    }
                    Ok(ws)
                })
            })),
        )
        .await
    }
}

pub struct KrakenClientBuilder {
    symbols: Vec<String>,
    base_url: String,
    enable_quote: bool,
    enable_trade: bool,
}

impl Default for KrakenClientBuilder {
    fn default() -> Self {
        Self {
            symbols: vec![],
            base_url: DEFAULT_KRAKEN_WS_URL.to_string(),
            enable_quote: false,
            enable_trade: false,
        }
    }
}

impl KrakenClientBuilder {
    pub fn add_symbols(mut self, symbols: Vec<impl Into<String>>) -> Self {
        self.symbols.extend(symbols.into_iter().map(|s| s.into()));
        self
    }

    pub fn with_quotes(mut self, enable: bool) -> Self {
        self.enable_quote = enable;
        self
    }

    pub fn with_trades(mut self, enable: bool) -> Self {
        self.enable_trade = enable;
        self
    }

    pub fn with_base_url(mut self, base_url: String) -> Self {
        self.base_url = base_url;
        self
    }

    pub fn build(self) -> eyre::Result<KrakenClient> {
        Ok(KrakenClient {
            symbols: self.symbols,
            base_url: self.base_url,
            enable_quote: self.enable_quote,
            enable_trade: self.enable_trade,
        })
    }
}
