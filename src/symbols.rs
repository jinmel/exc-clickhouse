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

pub async fn fetch_binance_spot_symbols() -> eyre::Result<Vec<String>> {
    const BINANCE_URL: &str = "https://api.binance.com/api/v3/exchangeInfo";

    let resp = reqwest::get(BINANCE_URL).await?.error_for_status()?;
    let info: BinanceExchangeInfo = resp.json().await?;

    let symbols = info
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
    Ok(symbols)
}
