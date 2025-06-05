use serde::{Deserialize, Serialize};
use eyre::WrapErr;

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
}
