use crate::symbols::{SymbolsConfig, SymbolsConfigEntry, fetch_binance_top_spot_pairs};
use clap::Parser;
use dotenv::dotenv;
use eyre::WrapErr;
use futures::{StreamExt, pin_mut};
use std::time::Duration;
use tokio::sync::mpsc;
use tower::{Service, ServiceBuilder, ServiceExt};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

use crate::{
    clickhouse::{ClickHouseConfig, ClickHouseService},
    config::{AppConfig, Cli, Commands, DbCommands, StreamArgs},
    models::{ClickhouseMessage, NormalizedEvent},
    streams::{
        ExchangeClient, WebsocketStream, binance::BinanceClient, bybit::BybitClient,
        coinbase::CoinbaseClient, kraken::KrakenClient, kucoin::KucoinClient, okx::OkxClient,
    },
    task_manager::{TaskManager, IntoTaskResult},
};

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
    // Create app configuration from stream args
    let app_config = AppConfig::from_stream_args(&args, "info")?;

    let symbols_cfg = crate::config::read_symbols(&args.symbols_file)?;

    let binance_symbols: Vec<String> = symbols_cfg
        .entries
        .iter()
        .filter(|e| e.exchange.eq_ignore_ascii_case("binance"))
        .flat_map(|e| e.symbols.iter().cloned().map(|s| s.to_lowercase()))
        .collect();

    let bybit_symbols: Vec<String> = symbols_cfg
        .entries
        .iter()
        .filter(|e| e.exchange.eq_ignore_ascii_case("bybit"))
        .flat_map(|e| e.symbols.iter().cloned())
        .collect();

    let okx_symbols: Vec<String> = symbols_cfg
        .entries
        .iter()
        .filter(|e| e.exchange.eq_ignore_ascii_case("okx"))
        .flat_map(|e| e.symbols.iter().cloned())
        .collect();

    let coinbase_symbols: Vec<String> = symbols_cfg
        .entries
        .iter()
        .filter(|e| e.exchange.eq_ignore_ascii_case("coinbase"))
        .flat_map(|e| e.symbols.iter().cloned())
        .collect();

    let kraken_symbols: Vec<String> = symbols_cfg
        .entries
        .iter()
        .filter(|e| e.exchange.eq_ignore_ascii_case("kraken"))
        .flat_map(|e| e.symbols.iter().cloned())
        .collect();

    let kucoin_symbols: Vec<String> = symbols_cfg
        .entries
        .iter()
        .filter(|e| e.exchange.eq_ignore_ascii_case("kucoin"))
        .flat_map(|e| e.symbols.iter().cloned())
        .collect();

    // Check if any data producer tasks will be enabled
    let has_producers = app_config.has_exchange_symbols()
        || app_config.ethereum_config.enabled
        || app_config.timeboost_config.enabled;

    if !has_producers {
        tracing::warn!("No tasks were enabled. Use --help to see available options.");
        return Ok(());
    }

    // Create channel for task communication
    let (msg_tx, msg_rx) = mpsc::unbounded_channel::<ClickhouseMessage>();

    // Create TaskManager with restart configuration from AppConfig
    let task_config = app_config.get_task_manager_config();

    let mut task_manager = TaskManager::<()>::with_config(task_config);



    // Spawn tasks
    if !binance_symbols.is_empty() {
        let symbols = binance_symbols.clone();
        let tx = msg_tx.clone();

        tracing::info!("Spawning binance stream for symbols: {:?}", symbols);
        task_manager.spawn_task(TaskName::BinanceStream, move || async move {
            binance_stream_task(tx, symbols).await.into_task_result()
        });
    }

    if !bybit_symbols.is_empty() {
        let symbols = bybit_symbols.clone();
        let tx = msg_tx.clone();

        tracing::info!("Spawning bybit stream for symbols: {:?}", symbols);
        task_manager.spawn_task(TaskName::BybitStream, move || async move {
            bybit_stream_task(tx, symbols).await.into_task_result()
        });
    }

    if !okx_symbols.is_empty() {
        let symbols = okx_symbols.clone();
        let tx = msg_tx.clone();

        tracing::info!("Spawning okx stream for symbols: {:?}", symbols);
        task_manager.spawn_task(TaskName::OkxStream, move || async move {
            okx_stream_task(tx, symbols).await.into_task_result()
        });
    }

    if !coinbase_symbols.is_empty() {
        let symbols = coinbase_symbols.clone();
        let tx = msg_tx.clone();

        tracing::info!("Spawning coinbase stream for symbols: {:?}", symbols);
        task_manager.spawn_task(TaskName::CoinbaseStream, move || async move {
            coinbase_stream_task(tx, symbols).await.into_task_result()
        });
    }

    if !kraken_symbols.is_empty() {
        let symbols = kraken_symbols.clone();
        let tx = msg_tx.clone();

        tracing::info!("Spawning kraken stream for symbols: {:?}", symbols);
        task_manager.spawn_task(TaskName::KrakenStream, move || async move {
            kraken_stream_task(tx, symbols).await.into_task_result()
        });
    }

    if !kucoin_symbols.is_empty() {
        let symbols = kucoin_symbols.clone();
        let tx = msg_tx.clone();

        tracing::info!("Spawning kucoin stream for symbols: {:?}", symbols);
        task_manager.spawn_task(TaskName::KucoinStream, move || async move {
            kucoin_stream_task(tx, symbols).await.into_task_result()
        });
    }

    if app_config.ethereum_config.enabled {
        let rpc_url = app_config.get_rpc_url()?;

        tracing::info!(
            "Spawning ethereum block metadata task from RPC URL: {}",
            rpc_url
        );
        let tx = msg_tx.clone();
        task_manager.spawn_task(
            TaskName::EthereumBlockMetadata,
            move || async move { ethereum::fetch_blocks_task(rpc_url, tx).await.into_task_result() }
        );
    }

    // Automatically spawn ClickHouse writer if we have any data producers
    if has_producers {
        tracing::info!("Spawning clickhouse writer task (auto-enabled for producer tasks)");
        task_manager.spawn_task(TaskName::ClickHouseWriter, move || async move {
            clickhouse_writer_task(msg_rx, args.clickhouse_rate_limit, args.batch_size).await.into_task_result()
        });
    }

    if app_config.timeboost_config.enabled {
        tracing::info!("Spawning timeboost bids task");
        let tx = msg_tx.clone();
        task_manager.spawn_task(TaskName::TimeboostBids, move || async move {
            timeboost::bids::fetch_bids_task(tx).await.into_task_result()
        });
    }

    // Drop the sender to ensure proper cleanup
    drop(msg_tx);

    // Monitor tasks using TaskManager with restart capabilities
    tokio::select! {
        result = task_manager.run() => {
            match result {
                Ok(()) => {
                    tracing::info!("TaskManager completed successfully");
                }
                Err(e) => {
                    tracing::error!("TaskManager failed: {:?}", e);
                }
            }
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("SIGINT received; initiating graceful shutdown");
            if let Err(e) = task_manager.shutdown().await {
                tracing::error!("Error during TaskManager shutdown: {:?}", e);
            }
        }
    }

    tracing::info!("Application shutdown complete");
    Ok(())
}

/// Generic stream processing function that works with any WebsocketStream + ExchangeClient implementation
/// This eliminates the code duplication in the exchange-specific stream task functions
async fn process_exchange_stream<T>(
    client: T,
    evt_tx: mpsc::UnboundedSender<ClickhouseMessage>,
) -> eyre::Result<()>
where
    T: WebsocketStream + ExchangeClient,
    T::Error: std::error::Error + Send + Sync + 'static,
{
    let exchange_name = client.get_exchange_name();
    let symbols = client.get_symbols();

    tracing::info!(
        "Starting {} stream for {} symbols: {:?}",
        exchange_name,
        symbols.len(),
        symbols
    );

    let combined_stream = client
        .stream_events()
        .await
        .map_err(|e| eyre::eyre!("Failed to create stream: {}", e))?;
    let stream =
        combined_stream.filter_map(|event: Result<NormalizedEvent, T::Error>| async move {
            match event {
                Ok(NormalizedEvent::Trade(trade)) => {
                    Some(ClickhouseMessage::Cex(NormalizedEvent::Trade(trade)))
                }
                Ok(NormalizedEvent::Quote(quote)) => {
                    Some(ClickhouseMessage::Cex(NormalizedEvent::Quote(quote)))
                }
                Err(e) => {
                    tracing::error!("Error streaming {} event: {:?}", exchange_name, e);
                    None
                }
            }
        });
    pin_mut!(stream);
    while let Some(msg) = stream.next().await {
        if evt_tx.send(msg).is_err() {
            tracing::error!("Failed to send message to channel");
        }
    }
    Ok(())
}

async fn binance_stream_task(
    evt_tx: mpsc::UnboundedSender<ClickhouseMessage>,
    symbols: Vec<String>,
) -> eyre::Result<()> {
    let client = BinanceClient::builder().add_symbols(symbols).build()?;
    process_exchange_stream(client, evt_tx).await
}

async fn bybit_stream_task(
    evt_tx: mpsc::UnboundedSender<ClickhouseMessage>,
    symbols: Vec<String>,
) -> eyre::Result<()> {
    let client = BybitClient::builder().add_symbols(symbols).build()?;
    process_exchange_stream(client, evt_tx).await
}

async fn okx_stream_task(
    evt_tx: mpsc::UnboundedSender<ClickhouseMessage>,
    symbols: Vec<String>,
) -> eyre::Result<()> {
    let client = OkxClient::builder().add_symbols(symbols).build()?;
    process_exchange_stream(client, evt_tx).await
}

async fn coinbase_stream_task(
    evt_tx: mpsc::UnboundedSender<ClickhouseMessage>,
    symbols: Vec<String>,
) -> eyre::Result<()> {
    let client = CoinbaseClient::builder().add_symbols(symbols).build()?;
    process_exchange_stream(client, evt_tx).await
}

async fn kraken_stream_task(
    evt_tx: mpsc::UnboundedSender<ClickhouseMessage>,
    symbols: Vec<String>,
) -> eyre::Result<()> {
    let client = KrakenClient::builder().add_symbols(symbols).build()?;
    process_exchange_stream(client, evt_tx).await
}

async fn kucoin_stream_task(
    evt_tx: mpsc::UnboundedSender<ClickhouseMessage>,
    symbols: Vec<String>,
) -> eyre::Result<()> {
    let client = KucoinClient::builder().add_symbols(symbols).build().await?;
    process_exchange_stream(client, evt_tx).await
}

async fn clickhouse_writer_task(
    mut rx: mpsc::UnboundedReceiver<ClickhouseMessage>,
    rate_limit: u64,
    batch_size: usize,
) -> eyre::Result<()> {
    let cfg = ClickHouseConfig::from_env()?;
    let clickhouse_svc = ClickHouseService::new(cfg);
    let mut service = ServiceBuilder::new()
        .rate_limit(rate_limit, Duration::from_secs(1))
        .service(clickhouse_svc);
    let mut buffer = Vec::with_capacity(batch_size);

    while rx.recv_many(&mut buffer, batch_size).await > 0 {
        service.ready().await?;
        service.call(std::mem::take(&mut buffer)).await?;
    }

    if !buffer.is_empty() {
        service.ready().await?;
        service.call(buffer).await?;
    }
    Ok(())
}
