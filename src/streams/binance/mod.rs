use async_trait::async_trait;
use url::Url;

use crate::{
    models::NormalizedEvent,
    streams::{CombinedStream, ExchangeStream, ExchangeStreamError},
};

/// Default WebSocket URL for Binance
pub const DEFAULT_BINANCE_WS_URL: &str = "wss://stream.binance.com:9443/stream";

pub mod model;
pub mod parser;

pub struct BinanceClient {
    symbols: Vec<String>,
    base_url: String,
    enable_quote: bool,
    enable_trade: bool,
}

impl BinanceClient {
    /// Creates a new BinanceBuilder with default values
    pub fn builder() -> BinanceClientBuilder {
        BinanceClientBuilder::default()
    }

    fn build_multi_stream_url(&self) -> Result<String, ExchangeStreamError> {
        let stream_name_part = self.symbols.iter().flat_map(|symbol| {
            let mut stream_names = vec![];
            if self.enable_quote {
                stream_names.push(format!("{symbol}@bookTicker"));
            }

            if self.enable_trade {
                stream_names.push(format!("{symbol}@trade"));
            }

            stream_names
        }).collect::<Vec<String>>().join("/");

        Url::parse_with_params(&self.base_url, &[("streams", &stream_name_part)])
            .map_err(|e| ExchangeStreamError::ParseError(format!("Failed to parse URL: {e}")))
            .map(|url| url.to_string())
    }
}

#[async_trait]
impl CombinedStream for BinanceClient {
    type CombinedStream = ExchangeStream<NormalizedEvent>;

    async fn combined_stream(&self) -> Result<Self::CombinedStream, ExchangeStreamError> {
        let url = self.build_multi_stream_url()?;
        ExchangeStream::new(&url, parser::parse_binance_combined).await
    }
}

/// Builder for the Binance struct
pub struct BinanceClientBuilder {
    symbols: Vec<String>,
    base_url: String,
    enable_quote: bool,
    enable_trade: bool,
}

impl Default for BinanceClientBuilder {
    fn default() -> Self {
        Self {
            symbols: vec![],
            base_url: DEFAULT_BINANCE_WS_URL.to_string(),
            enable_quote: false,
            enable_trade: false,
        }
    }
}

impl BinanceClientBuilder {
    pub fn add_symbol(mut self, symbol: impl Into<String>) -> Self {
        self.symbols.push(symbol.into());
        self
    }

    pub fn add_symbols(mut self, symbols: Vec<impl Into<String>>) -> Self {
        self.symbols.extend(symbols.into_iter().map(|s| s.into().to_lowercase()));
        self
    }

    /// Set the base URL
    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
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

    /// Build the Binance instance
    pub fn build(self) -> eyre::Result<BinanceClient> {
        Ok(BinanceClient {
            symbols: self.symbols,
            base_url: self.base_url,
            enable_quote: self.enable_quote,
            enable_trade: self.enable_trade,
        })
    }
}
