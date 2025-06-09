use crate::symbols::{SymbolsConfig, SymbolsConfigEntry, fetch_binance_top_spot_pairs};
use clap::Parser;
use dotenv::dotenv;
use eyre::WrapErr;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

use crate::app_context::AppBuilder;
use crate::{
    clickhouse::{ClickHouseConfig, ClickHouseService},
    config::{Cli, Commands, DbCommands, StreamArgs},
};

mod app_context;
mod clickhouse;
mod config;
mod ethereum;
mod models;
mod streams;
mod symbols;
mod task_manager;
mod timeboost;
mod tower_utils;

/// Enum for task names to ensure type safety and consistency
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TaskName {
    BinanceStream,
    BybitStream,
    OkxStream,
    CoinbaseStream,
    KrakenStream,
    KucoinStream,
    EthereumBlockMetadata,
    ClickHouseWriter,
    TimeboostBids,
}

impl std::fmt::Display for TaskName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskName::BinanceStream => write!(f, "Binance Stream"),
            TaskName::BybitStream => write!(f, "Bybit Stream"),
            TaskName::OkxStream => write!(f, "OKX Stream"),
            TaskName::CoinbaseStream => write!(f, "Coinbase Stream"),
            TaskName::KrakenStream => write!(f, "Kraken Stream"),
            TaskName::KucoinStream => write!(f, "KuCoin Stream"),
            TaskName::EthereumBlockMetadata => write!(f, "Ethereum Block Metadata"),
            TaskName::ClickHouseWriter => write!(f, "ClickHouse Writer"),
            TaskName::TimeboostBids => write!(f, "Timeboost Bids"),
        }
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    // Parse command line arguments
    let cli = Cli::parse();

    // Load environment variables from .env file
    dotenv().ok();

    // Initialize tracing with environment filter using CLI log level
    let log_level = format!("exc_clickhouse={},info", cli.log_level);
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&log_level)),
        )
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    match cli.command {
        Commands::Db(db_cmd) => match db_cmd.command {
            DbCommands::Timeboost => {
                tracing::info!("Backfilling timeboost bids");
                timeboost::bids::backfill_timeboost_bids().await?;
                tracing::info!("Timeboost bids backfill complete");
                return Ok(());
            }
            DbCommands::FetchBinanceSymbols(args) => {
                tracing::info!("Fetching top Binance SPOT symbols");
                let pairs = fetch_binance_top_spot_pairs(args.limit).await?;
                let symbols: Vec<String> = pairs.iter().map(|p| p.pair.clone()).collect();
                let cfg = SymbolsConfig {
                    entries: vec![SymbolsConfigEntry {
                        exchange: "Binance".to_string(),
                        market: "SPOT".to_string(),
                        symbols,
                    }],
                };
                let file = std::fs::File::create(&args.output)
                    .wrap_err("Failed to create output YAML file")?;
                cfg.to_yaml(file)?;
                tracing::info!("Symbols written to {}", args.output);

                if args.update_clickhouse {
                    tracing::info!("Updating ClickHouse trading_pairs table");
                    let ch_cfg = ClickHouseConfig::from_env()?;
                    let ch = ClickHouseService::new(ch_cfg);
                    ch.write_trading_pairs(pairs).await?;
                }

                return Ok(());
            }
        },
        Commands::Stream(args) => run_stream(args).await,
    }
}

async fn run_stream(args: StreamArgs) -> eyre::Result<()> {
    // Build and run the application using the fluent interface
    let app_context = AppBuilder::from_stream_args(&args, "info")?.build()?;
    app_context.run().await
}
