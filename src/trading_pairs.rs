use crate::clickhouse::{ClickHouseConfig, ClickHouseService};
use clickhouse::Row;
use eyre::WrapErr;
use serde::{Deserialize, Serialize};
use std::fs::File;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TradingPairsConfig {
    pub trading_pairs: Vec<TradingPair>,
}

impl TradingPairsConfig {
    pub fn from_yaml<R: std::io::Read>(reader: R) -> eyre::Result<Self> {
        let trading_pairs: Vec<TradingPair> =
            serde_yaml::from_reader(reader).wrap_err("Failed to parse trading_pairs")?;
        Ok(Self { trading_pairs })
    }
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct TradingPair {
    pub exchange: String,
    pub trading_type: String,
    pub pair: String,
    pub base_asset: String,
    pub quote_asset: String,
}

impl TradingPair {
    /// Normalize a trading pair by converting exchange name to lowercase
    fn normalize(mut self) -> Self {
        self.exchange = self.exchange.to_lowercase();
        if self.exchange == "okx" {
            self.exchange = "okex".to_string();
        }
        self.trading_type = self.trading_type.to_uppercase();
        self
    }
}

pub async fn backfill_trading_pairs(trading_pairs_file: &str) -> eyre::Result<()> {
    let trading_pairs = TradingPairsConfig::from_yaml(File::open(trading_pairs_file)?)?;
    let trading_pairs_normalized: Vec<TradingPair> = trading_pairs
        .trading_pairs
        .into_iter()
        .map(TradingPair::normalize)
        .collect();

    let clickhouse = ClickHouseService::new(ClickHouseConfig::from_env()?);
    clickhouse
        .write_trading_pairs(trading_pairs_normalized)
        .await?;
    Ok(())
}
