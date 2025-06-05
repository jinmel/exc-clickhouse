use async_trait::async_trait;
use futures::SinkExt;
use tokio_tungstenite::tungstenite::Message;

use crate::{
    models::NormalizedEvent,
    streams::{CombinedStream, ExchangeStream, ExchangeStreamError},
};

pub const DEFAULT_BYBIT_WS_URL: &str = "wss://stream.bybit.com/v5/public/spot";

pub mod model;
pub mod parser;

pub struct BybitClient {
    symbols: Vec<String>,
    base_url: String,
    enable_quote: bool,
    enable_trade: bool,
}

impl BybitClient {
    pub fn builder() -> BybitClientBuilder {
        BybitClientBuilder::default()
    }

    fn build_subscription_message(&self) -> Result<serde_json::Value, ExchangeStreamError> {
        if self.symbols.is_empty() {
            return Err(ExchangeStreamError::InvalidConfiguration(
                "No symbols provided".to_string(),
            ));
        }
        let mut args = Vec::new();
        for symbol in &self.symbols {
            if self.enable_trade {
                args.push(format!("publicTrade.{symbol}"));
            }
            if self.enable_quote {
                args.push(format!("tickers.{symbol}"));
            }
        }
        Ok(serde_json::json!({"op": "subscribe", "args": args}))
    }
}

#[async_trait]
impl CombinedStream for BybitClient {
    type CombinedStream = ExchangeStream<NormalizedEvent>;

    async fn combined_stream(&self) -> Result<Self::CombinedStream, ExchangeStreamError> {
        let subscribe_msg = self.build_subscription_message()?;
        ExchangeStream::new(
            &self.base_url,
            parser::parse_bybit_combined,
            None,
            Some(Box::new(move |mut ws| {
                Box::pin(async move {
                    ws.send(Message::Text(serde_json::to_string(&subscribe_msg).unwrap().into()))
                        .await
                        .map_err(|e| ExchangeStreamError::StreamError(e.to_string()))?;
                    Ok(ws)
                })
            })),
        )
        .await
    }
}

pub struct BybitClientBuilder {
    symbols: Vec<String>,
    base_url: String,
    enable_quote: bool,
    enable_trade: bool,
}

impl Default for BybitClientBuilder {
    fn default() -> Self {
        Self {
            symbols: vec![],
            base_url: DEFAULT_BYBIT_WS_URL.to_string(),
            enable_quote: false,
            enable_trade: false,
        }
    }
}

impl BybitClientBuilder {
    pub fn add_symbols(mut self, symbols: Vec<impl Into<String>>) -> Self {
        self
            .symbols
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

    pub fn build(self) -> eyre::Result<BybitClient> {
        Ok(BybitClient {
            symbols: self.symbols,
            base_url: self.base_url,
            enable_quote: self.enable_quote,
            enable_trade: self.enable_trade,
        })
    }
}

