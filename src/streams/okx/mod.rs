use async_trait::async_trait;
use futures::SinkExt;
use tokio_tungstenite::tungstenite::Message;

use crate::{
    models::NormalizedEvent,
    streams::{CombinedStream, ExchangeStream, ExchangeStreamError},
};

pub const DEFAULT_OKX_WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";

pub mod model;
pub mod parser;

pub struct OkxClient {
    symbols: Vec<String>,
    base_url: String,
    enable_quote: bool,
    enable_trade: bool,
}

impl OkxClient {
    pub fn builder() -> OkxClientBuilder {
        OkxClientBuilder::default()
    }

    fn build_subscription_message(&self) -> Result<serde_json::Value, ExchangeStreamError> {
        if self.symbols.is_empty() {
            return Err(ExchangeStreamError::InvalidConfiguration(
                "No symbols provided".to_string(),
            ));
        }

        let args: Vec<serde_json::Value> = self
            .symbols
            .iter()
            .flat_map(|symbol| {
                let mut out = Vec::new();
                if self.enable_trade {
                    out.push(serde_json::json!({"channel": "trades", "instId": symbol}));
                }
                if self.enable_quote {
                    out.push(serde_json::json!({"channel": "tickers", "instId": symbol}));
                }
                out
            })
            .collect();

        Ok(serde_json::json!({"op": "subscribe", "args": args}))
    }
}

#[async_trait]
impl CombinedStream for OkxClient {
    type CombinedStream = ExchangeStream<NormalizedEvent>;

    async fn combined_stream(&self) -> Result<Self::CombinedStream, ExchangeStreamError> {
        let subscribe_msg = self.build_subscription_message()?;
        ExchangeStream::new(
            &self.base_url,
            parser::parse_okx_combined,
            None,
            Some(Box::new(move |mut ws| {
                Box::pin(async move {
                    ws.send(Message::Text(
                        serde_json::to_string(&subscribe_msg).unwrap().into(),
                    ))
                    .await
                    .map_err(|e| ExchangeStreamError::StreamError(e.to_string()))?;
                    Ok(ws)
                })
            })),
        )
        .await
    }
}

pub struct OkxClientBuilder {
    symbols: Vec<String>,
    base_url: String,
    enable_quote: bool,
    enable_trade: bool,
}

impl Default for OkxClientBuilder {
    fn default() -> Self {
        Self {
            symbols: vec![],
            base_url: DEFAULT_OKX_WS_URL.to_string(),
            enable_quote: false,
            enable_trade: false,
        }
    }
}

impl OkxClientBuilder {
    pub fn add_symbols(mut self, symbols: Vec<impl Into<String>>) -> Self {
        self.symbols
            .extend(symbols.into_iter().map(|s| s.into().to_uppercase()));
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

    pub fn build(self) -> eyre::Result<OkxClient> {
        Ok(OkxClient {
            symbols: self.symbols,
            base_url: self.base_url,
            enable_quote: self.enable_quote,
            enable_trade: self.enable_trade,
        })
    }
}
