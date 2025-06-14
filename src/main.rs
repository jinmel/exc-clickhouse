use clap::Parser;
use dotenv::dotenv;
use futures::{StreamExt, pin_mut};
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, Mutex};
use tower::{Service, ServiceBuilder, ServiceExt};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

use crate::{
    cli::{Cli, Commands, DbCommands, StreamArgs},
    clickhouse::{ClickHouseConfig, ClickHouseService},
    config::AppConfig,
    models::{ClickhouseMessage, NormalizedEvent},
    streams::{
        ExchangeClient, WebsocketStream, binance::BinanceClient, bybit::BybitClient,
        coinbase::CoinbaseClient, kraken::KrakenClient, kucoin::KucoinClient, okx::OkxClient,
    },
    task_manager::{IntoTaskResult, TaskManager},
};

mod cli;
mod clickhouse;
mod config;
mod ethereum;
mod models;
mod streams;
mod task_manager;
mod timeboost;
mod tower_utils;
mod trading_pairs;

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
            DbCommands::TradingPairs(args) => {
                tracing::info!("Backfilling trading pairs");
                trading_pairs::backfill_trading_pairs(&args.trading_pairs_file).await?;
                tracing::info!("Trading pairs backfill complete");
                return Ok(());
            }
        },
        Commands::Stream(args) => run_stream(args).await,
    }
}

async fn run_stream(args: StreamArgs) -> eyre::Result<()> {
    // Create app configuration from stream args
    let app_config = AppConfig::from_stream_args(&args, "info")?;

    let binance_symbols: Vec<String> = app_config.exchange_configs.binance_symbols.clone();
    let bybit_symbols: Vec<String> = app_config.exchange_configs.bybit_symbols.clone();
    let okx_symbols: Vec<String> = app_config.exchange_configs.okx_symbols.clone();
    let coinbase_symbols: Vec<String> = app_config.exchange_configs.coinbase_symbols.clone();
    let kraken_symbols: Vec<String> = app_config.exchange_configs.kraken_symbols.clone();
    let kucoin_symbols: Vec<String> = app_config.exchange_configs.kucoin_symbols.clone();

    // Check if any data producer tasks will be enabled
    let has_producers = app_config.has_exchange_symbols()
        || app_config.ethereum_config.enabled
        || app_config.timeboost_config.enabled;

    if !has_producers {
        tracing::warn!(
            "No tasks were enabled and all exchange symbols are empty. Use --help to see available options."
        );
        return Ok(());
    }

    // Create channel for task communication
    let (msg_tx, msg_rx) = mpsc::unbounded_channel::<ClickhouseMessage>();

    // Create TaskManager with restart configuration from AppConfig
    let task_config = app_config.get_task_manager_config();
    let mut task_manager = TaskManager::<()>::with_config(task_config);

    // Spawn tasks
    if !binance_symbols.is_empty() {
        let tx = msg_tx.clone();
        let client = BinanceClient::builder().add_symbols(binance_symbols).build()?;
        task_manager.spawn_task(TaskName::BinanceStream, move || {
            let client = client.clone();
            let tx = tx.clone();
            Box::pin(async move { process_exchange_stream(client, tx).await.into_task_result() })
        });
    }

    if !bybit_symbols.is_empty() {
        let tx = msg_tx.clone();
        let client = BybitClient::builder().add_symbols(bybit_symbols).build()?;
        task_manager.spawn_task(TaskName::BybitStream, move || {
            let client = client.clone();
            let tx = tx.clone();
            Box::pin(async move { process_exchange_stream(client, tx).await.into_task_result() })
        });
    }

    if !okx_symbols.is_empty() {
        let tx = msg_tx.clone();
        let client = OkxClient::builder().add_symbols(okx_symbols).build()?;
        task_manager.spawn_task(TaskName::OkxStream, move || {
            let client = client.clone();
            let tx = tx.clone();
            Box::pin(async move { process_exchange_stream(client, tx).await.into_task_result() })
        });
    }

    if !coinbase_symbols.is_empty() {
        let tx = msg_tx.clone();
        let client = CoinbaseClient::builder().add_symbols(coinbase_symbols).build()?;
        task_manager.spawn_task(TaskName::CoinbaseStream, move || {
            let client = client.clone();
            let tx = tx.clone();
            Box::pin(async move { process_exchange_stream(client, tx).await.into_task_result() })
        });
    }

    if !kraken_symbols.is_empty() {
        let tx = msg_tx.clone();
        let client = KrakenClient::builder().add_symbols(kraken_symbols).build()?;
        task_manager.spawn_task(TaskName::KrakenStream, move || {
            let client = client.clone();
            let tx = tx.clone();
            Box::pin(async move { process_exchange_stream(client, tx).await.into_task_result() })
        });
    }

    if !kucoin_symbols.is_empty() {
        let symbols = kucoin_symbols.clone();
        let tx = msg_tx.clone();
        let client = KucoinClient::builder().add_symbols(symbols).build().await?;
        task_manager.spawn_task(TaskName::KucoinStream, move || {
            let client = client.clone();
            let tx = tx.clone();
            Box::pin(async move { process_exchange_stream(client, tx).await.into_task_result() })
        });
    }

    if app_config.ethereum_config.enabled {
        let rpc_url = app_config.get_rpc_url()?;

        tracing::info!(
            "Spawning ethereum block metadata task from RPC URL: {}",
            rpc_url
        );
        let tx = msg_tx.clone();
        task_manager.spawn_task(TaskName::EthereumBlockMetadata, move || {
            let rpc_url = rpc_url.clone();
            let tx = tx.clone();
            Box::pin(async move { ethereum::fetch_blocks_task(rpc_url, tx).await.into_task_result() })
        });
    }

    // Automatically spawn ClickHouse writer if we have any data producers
    if has_producers {
        tracing::info!("Spawning clickhouse writer task (auto-enabled for producer tasks)");
        let msg_rx = Arc::new(Mutex::new(msg_rx));
        task_manager.spawn_task(TaskName::ClickHouseWriter, move || {   
            let rx = msg_rx.clone();
            let rate_limit = args.clickhouse_rate_limit;
            let batch_size = args.batch_size;
            Box::pin(async move { clickhouse_writer_task(rx, rate_limit, batch_size).await.into_task_result() })
        });
    }

    if app_config.timeboost_config.enabled {
        tracing::info!("Spawning timeboost bids task");
        let tx = msg_tx.clone();
        task_manager.spawn_task(TaskName::TimeboostBids, move || {
            let tx = tx.clone();
            Box::pin(async move { timeboost::bids::fetch_bids_task(tx).await.into_task_result() })
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

async fn clickhouse_writer_task(
    rx: Arc<Mutex<mpsc::UnboundedReceiver<ClickhouseMessage>>>,
    rate_limit: u64,
    batch_size: usize,
) -> eyre::Result<()> {
    let cfg = ClickHouseConfig::from_env()?;
    let clickhouse_svc = ClickHouseService::new(cfg);
    let mut service = ServiceBuilder::new()
        .rate_limit(rate_limit, Duration::from_secs(1))
        .service(clickhouse_svc);
    let mut buffer = Vec::with_capacity(batch_size);
    let mut rx = rx.lock().await;

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
