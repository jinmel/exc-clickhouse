use eyre::WrapErr;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::time::Duration;

use crate::cli::StreamArgs;
use crate::trading_pairs::TradingPairsConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub log_level: String,
    pub symbols_file: String,
    pub batch_size: usize,
    pub clickhouse_rate_limit: u64,
    pub restart_config: RestartConfig,
    pub ethereum_config: EthereumConfig,
    pub timeboost_config: TimeboostConfig,
    pub exchange_configs: ExchangeConfigs,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestartConfig {
    pub enabled: bool,
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EthereumConfig {
    pub enabled: bool,
    pub rpc_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeboostConfig {
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeConfigs {
    pub binance_symbols: Vec<String>,
    pub bybit_symbols: Vec<String>,
    pub okx_symbols: Vec<String>,
    pub coinbase_symbols: Vec<String>,
    pub kraken_symbols: Vec<String>,
    pub kucoin_symbols: Vec<String>,
    pub mexc_symbols: Vec<String>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            log_level: "info".to_string(),
            symbols_file: "symbols.yaml".to_string(),
            batch_size: 500,
            clickhouse_rate_limit: 5,
            restart_config: RestartConfig::default(),
            ethereum_config: EthereumConfig::default(),
            timeboost_config: TimeboostConfig::default(),
            exchange_configs: ExchangeConfigs::default(),
        }
    }
}

impl Default for RestartConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_attempts: 0,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(300),
        }
    }
}

impl Default for EthereumConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            rpc_url: None,
        }
    }
}

impl Default for TimeboostConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

impl Default for ExchangeConfigs {
    fn default() -> Self {
        Self {
            binance_symbols: Vec::new(),
            bybit_symbols: Vec::new(),
            okx_symbols: Vec::new(),
            coinbase_symbols: Vec::new(),
            kraken_symbols: Vec::new(),
            kucoin_symbols: Vec::new(),
            mexc_symbols: Vec::new(),
        }
    }
}

impl AppConfig {
    /// Create AppConfig from CLI arguments and symbols file
    pub fn from_stream_args(args: &StreamArgs, cli_log_level: &str) -> eyre::Result<Self> {
        let trading_pairs = read_trading_pairs(&args.trading_pairs_file)?;
        let exchange_configs = ExchangeConfigs::from_trading_pairs_config(&trading_pairs);

        Ok(Self {
            log_level: cli_log_level.to_string(),
            symbols_file: args.trading_pairs_file.clone(),
            batch_size: args.batch_size,
            clickhouse_rate_limit: args.clickhouse_rate_limit,
            restart_config: RestartConfig {
                enabled: args.enable_restart,
                max_attempts: args.max_restart_attempts,
                initial_delay: Duration::from_secs(args.restart_delay_seconds),
                max_delay: Duration::from_secs(args.max_restart_delay_seconds),
            },
            ethereum_config: EthereumConfig {
                enabled: !args.skip_ethereum,
                rpc_url: args.rpc_url.clone(),
            },
            timeboost_config: TimeboostConfig {
                enabled: !args.skip_timeboost,
            },
            exchange_configs,
        })
    }

    /// Get the effective RPC URL from config or environment
    pub fn get_rpc_url(&self) -> eyre::Result<String> {
        self.ethereum_config
            .rpc_url
            .clone()
            .or_else(|| std::env::var("RPC_URL").ok())
            .ok_or_else(|| {
                eyre::eyre!(
                    "RPC_URL must be provided via --rpc-url flag or RPC_URL environment variable"
                )
            })
    }

    /// Check if any exchange has symbols configured
    pub fn has_exchange_symbols(&self) -> bool {
        !self.exchange_configs.binance_symbols.is_empty()
            || !self.exchange_configs.bybit_symbols.is_empty()
            || !self.exchange_configs.okx_symbols.is_empty()
            || !self.exchange_configs.coinbase_symbols.is_empty()
            || !self.exchange_configs.kraken_symbols.is_empty()
            || !self.exchange_configs.kucoin_symbols.is_empty()
            || !self.exchange_configs.mexc_symbols.is_empty()
    }

    /// Create TaskManagerConfig from the current AppConfig
    pub fn get_task_manager_config(&self) -> crate::task_manager::TaskManagerConfig {
        crate::task_manager::TaskManagerConfig {
            max_concurrent_tasks: 100, // Could be made configurable later
            default_restart_policy: crate::task_manager::RestartPolicy {
                max_restarts: if self.restart_config.enabled {
                    self.restart_config.max_attempts
                } else {
                    0
                },
                base_delay: self.restart_config.initial_delay,
                backoff_multiplier: 2.0,
                max_delay: self.restart_config.max_delay,
                jitter_factor: 0.1,
                enable_circuit_breaker: self.restart_config.enabled,
                circuit_breaker_threshold: 5,
                circuit_breaker_timeout: std::time::Duration::from_secs(60),
            },
            shutdown_timeout: std::time::Duration::from_secs(30),
            shutdown_config: crate::task_manager::ShutdownConfig::default(),
        }
    }
}

impl ExchangeConfigs {
    /// Create ExchangeConfigs from SymbolsConfig
    pub fn from_trading_pairs_config(cfg: &TradingPairsConfig) -> Self {
        // Every exchange has a different convention for its symbols
        let binance_symbols: Vec<String> = cfg
            .trading_pairs
            .iter()
            .filter(|e| e.exchange.eq_ignore_ascii_case("binance"))
            .filter(|e| e.trading_type.eq_ignore_ascii_case("spot"))
            // Binance accepts lowercase symbols only
            .map(|e| format!("{}{}", e.base_asset.to_lowercase(), e.quote_asset.to_lowercase()))
            .collect();

        let bybit_symbols: Vec<String> = cfg
            .trading_pairs
            .iter()
            .filter(|e| e.exchange.eq_ignore_ascii_case("bybit"))
            .filter(|e| e.trading_type.eq_ignore_ascii_case("spot"))
            .map(|e| format!("{}-{}", e.base_asset, e.quote_asset))
            .collect();

        let okx_symbols: Vec<String> = cfg
            .trading_pairs
            .iter()
            .filter(|e| e.exchange.eq_ignore_ascii_case("okx"))
            .filter(|e| e.trading_type.eq_ignore_ascii_case("spot"))
            .map(|e| format!("{}-{}", e.base_asset, e.quote_asset))
            .collect();

        let coinbase_symbols: Vec<String> = cfg
            .trading_pairs
            .iter()
            .filter(|e| e.exchange.eq_ignore_ascii_case("coinbase"))
            .filter(|e| e.trading_type.eq_ignore_ascii_case("spot"))
            .map(|e| format!("{}-{}", e.base_asset, e.quote_asset))
            .collect();

        let kraken_symbols: Vec<String> = cfg
            .trading_pairs
            .iter()
            .filter(|e| e.exchange.eq_ignore_ascii_case("kraken"))
            .filter(|e| e.trading_type.eq_ignore_ascii_case("spot"))
            .map(|e| format!("{}/{}", e.base_asset, e.quote_asset))
            .collect();

        let kucoin_symbols: Vec<String> = cfg
            .trading_pairs
            .iter()
            .filter(|e| e.exchange.eq_ignore_ascii_case("kucoin"))
            .filter(|e| e.trading_type.eq_ignore_ascii_case("spot"))
            .map(|e| format!("{}-{}", e.base_asset, e.quote_asset))
            .collect();

        let mexc_symbols: Vec<String> = cfg
            .trading_pairs
            .iter()
            .filter(|e| e.exchange.eq_ignore_ascii_case("mexc"))
            .filter(|e| e.trading_type.eq_ignore_ascii_case("spot"))
            // MEXC uses uppercase symbols without separators
            .map(|e| format!("{}{}", e.base_asset.to_uppercase(), e.quote_asset.to_uppercase()))
            .collect();

        Self {
            binance_symbols,
            bybit_symbols,
            okx_symbols,
            coinbase_symbols,
            kraken_symbols,
            kucoin_symbols,
            mexc_symbols,
        }
    }
}

/// Read symbols configuration from YAML file
pub fn read_trading_pairs(filename: &str) -> eyre::Result<TradingPairsConfig> {
    let file = File::open(filename).wrap_err("Failed to open symbols YAML file")?;
    TradingPairsConfig::from_yaml(file)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_config_default() {
        let config = AppConfig::default();
        assert_eq!(config.log_level, "info");
        assert_eq!(config.batch_size, 500);
        assert!(!config.restart_config.enabled);
    }

    #[test]
    fn test_restart_config_from_args() {
        let args = StreamArgs {
            trading_pairs_file: "test.yaml".to_string(),
            batch_size: 1000,
            skip_ethereum: false,
            skip_timeboost: false,
            rpc_url: None,
            enable_restart: true,
            max_restart_attempts: 5,
            restart_delay_seconds: 2,
            max_restart_delay_seconds: 600,
            clickhouse_rate_limit: 10,
        };

        let config = AppConfig::from(&args);
        assert!(config.restart_config.enabled);
        assert_eq!(config.restart_config.max_attempts, 5);
        assert_eq!(config.restart_config.initial_delay, Duration::from_secs(2));
        assert_eq!(config.restart_config.max_delay, Duration::from_secs(600));
    }

    #[test]
    fn test_read_symbols_file_not_found() {
        let result = read_trading_pairs("nonexistent.yaml");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_rpc_url_from_config() {
        let mut config = AppConfig::default();
        config.ethereum_config.rpc_url = Some("http://localhost:8545".to_string());

        let rpc_url = config.get_rpc_url().unwrap();
        assert_eq!(rpc_url, "http://localhost:8545");
    }

    #[test]
    fn test_has_exchange_symbols() {
        let mut config = AppConfig::default();
        assert!(!config.has_exchange_symbols());

        config
            .exchange_configs
            .binance_symbols
            .push("BTCUSDT".to_string());
        assert!(config.has_exchange_symbols());
    }
}

impl From<&StreamArgs> for AppConfig {
    fn from(args: &StreamArgs) -> Self {
        Self {
            log_level: "info".to_string(), // Will be set from CLI
            symbols_file: args.trading_pairs_file.clone(),
            batch_size: args.batch_size,
            clickhouse_rate_limit: args.clickhouse_rate_limit,
            restart_config: RestartConfig {
                enabled: args.enable_restart,
                max_attempts: args.max_restart_attempts,
                initial_delay: Duration::from_secs(args.restart_delay_seconds),
                max_delay: Duration::from_secs(args.max_restart_delay_seconds),
            },
            ethereum_config: EthereumConfig {
                enabled: true, // Default, will be overridden by skip flags
                rpc_url: args.rpc_url.clone(),
            },
            timeboost_config: TimeboostConfig {
                enabled: true, // Default, will be overridden by skip flags
            },
            exchange_configs: ExchangeConfigs::default(), // Will be populated from symbols file
        }
    }
}
