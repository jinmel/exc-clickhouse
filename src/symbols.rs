use eyre::WrapErr;
use serde::{Deserialize, Serialize};

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
}

#[derive(Debug, Deserialize)]
struct BinanceTickerInfo {
    symbol: String,
    #[serde(rename = "quoteVolume")]
    quote_volume: String,
}

pub async fn fetch_binance_spot_symbols() -> eyre::Result<Vec<String>> {
    const EXCHANGE_INFO_URL: &str = "https://data-api.binance.vision/api/v3/exchangeInfo";
    const TICKER_URL: &str = "https://data-api.binance.vision/api/v3/ticker/24hr";

    let client = reqwest::Client::new();

    let info: BinanceExchangeInfo = client
        .get(EXCHANGE_INFO_URL)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    use std::collections::HashSet;
    let mut spot: HashSet<String> = info
        .symbols
        .into_iter()
        .filter(|s| {
            if s.status != "TRADING" {
                return false;
            }
            if let Some(psets) = &s.permission_sets {
                if let Some(first) = psets.get(0) {
                    return first.iter().any(|p| p == "SPOT");
                }
            }
            false
        })
        .map(|s| s.symbol)
        .collect();

    let tickers: Vec<BinanceTickerInfo> = client
        .get(TICKER_URL)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let mut entries: Vec<(String, f64)> = tickers
        .into_iter()
        .filter(|t| spot.contains(&t.symbol))
        .map(|t| {
            let vol = t.quote_volume.parse::<f64>().unwrap_or(0.0);
            (t.symbol, vol)
        })
        .collect();

    entries.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    entries.truncate(200);

    Ok(entries.into_iter().map(|(s, _)| s).collect())
}
