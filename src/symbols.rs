use eyre::WrapErr;
use serde::{Deserialize, Serialize};
use clickhouse::Row;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SymbolsConfigEntry {
    pub exchange: String,
    pub market: String,
    pub symbols: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SymbolsConfig {
    pub entries: Vec<SymbolsConfigEntry>,
}

impl SymbolsConfig {
    pub fn from_yaml<R: std::io::Read>(reader: R) -> eyre::Result<Self> {
        let entries: Vec<SymbolsConfigEntry> =
            serde_yaml::from_reader(reader).wrap_err("Failed to parse YAML")?;
        Ok(Self { entries })
    }

    pub fn to_yaml<W: std::io::Write>(&self, writer: W) -> eyre::Result<()> {
        serde_yaml::to_writer(writer, self).wrap_err("Failed to write YAML")
    }
}

#[derive(Debug, Deserialize)]
struct BinanceExchangeInfo {
    symbols: Vec<BinanceSymbolInfo>,
}

#[derive(Debug, Deserialize)]
struct BinanceSymbolInfo {
    symbol: String,
    status: String,
    #[serde(rename = "permissionSets")]
    permission_sets: Option<Vec<Vec<String>>>,
    #[serde(rename = "baseAsset")]
    base_asset: String,
    #[serde(rename = "quoteAsset")]
    quote_asset: String,
}


#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct TradingPair {
    pub exchange: String,
    pub trading_type: String,
    pub pair: String,
    pub base_asset: String,
    pub quote_asset: String,
}

pub async fn fetch_binance_spot_pairs() -> eyre::Result<Vec<TradingPair>> {
    const EXCHANGE_INFO_URL: &str = "https://api.binance.com/api/v3/exchangeInfo";

    let client = reqwest::Client::new();

    let info: BinanceExchangeInfo = client
        .get(EXCHANGE_INFO_URL)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let mut pairs = Vec::new();
    for sym in info.symbols.into_iter() {
        if sym.status != "TRADING" {
            continue;
        }
        if let Some(psets) = &sym.permission_sets {
            if let Some(first) = psets.get(0) {
                if !first.iter().any(|p| p == "SPOT") {
                    continue;
                }
            } else {
                continue;
            }
        } else {
            continue;
        }

        pairs.push(TradingPair {
            exchange: "binance".to_string(),
            trading_type: "SPOT".to_string(),
            pair: sym.symbol,
            base_asset: sym.base_asset,
            quote_asset: sym.quote_asset,
        });
    }

    Ok(pairs)
}

#[allow(dead_code)]
pub async fn fetch_binance_spot_symbols() -> eyre::Result<Vec<String>> {
    Ok(
        fetch_binance_spot_pairs()
            .await?
            .into_iter()
            .map(|p| p.pair)
            .collect(),
    )
}
