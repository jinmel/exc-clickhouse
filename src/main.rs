use clap::Parser;
use dotenv::dotenv;
use futures::{StreamExt, pin_mut};
use rustls::crypto::ring::default_provider;
use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc};
use tower::{Service, ServiceBuilder, ServiceExt};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

use crate::{
    cli::{Cli, Commands, DbCommands, StreamArgs},
    clickhouse::{ClickHouseConfig, ClickHouseService},
    config::AppConfig,
    models::ClickhouseMessage,
    streams::{
        ExchangeClient, WebsocketStream, binance::BinanceClient, bybit::BybitClient,
        coinbase::CoinbaseClient, kraken::KrakenClient, kucoin::KucoinClient, okx::OkxClient,
        binancefutures::BinanceFuturesClient,
    },
    task_manager::{IntoTaskResult, TaskManager},
};

mod allium;
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
    BinanceFuturesStream,
    BybitStream,
    OkxStream,
    CoinbaseStream,
    KrakenStream,
    KucoinStream,
    EthereumBlockMetadata,
    ClickHouseWriter,
    TimeboostBids,
    AlliumDexVolumes,
}

impl std::fmt::Display for TaskName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskName::BinanceStream => write!(f, "Binance Stream"),
            TaskName::BinanceFuturesStream => write!(f, "Binance Futures Stream"),
            TaskName::BybitStream => write!(f, "Bybit Stream"),
            TaskName::OkxStream => write!(f, "OKX Stream"),
            TaskName::CoinbaseStream => write!(f, "Coinbase Stream"),
            TaskName::KrakenStream => write!(f, "Kraken Stream"),
            TaskName::KucoinStream => write!(f, "KuCoin Stream"),
            TaskName::EthereumBlockMetadata => write!(f, "Ethereum Block Metadata"),
            TaskName::ClickHouseWriter => write!(f, "ClickHouse Writer"),
            TaskName::TimeboostBids => write!(f, "Timeboost Bids"),
            TaskName::AlliumDexVolumes => write!(f, "Allium Dex Volumes"),
        }
    }
}

fn init_tracing(log_level: &str) -> eyre::Result<()> {
    let log_level = format!("exc_clickhouse={log_level},info");
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&log_level)),
        )
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    default_provider()
        .install_default()
        .expect("provider already installed");
    // Load environment variables from .env file
    dotenv()?;

    // Parse command line arguments
    let cli = Cli::parse();

    match cli.command {
        Commands::Db(db_cmd) => {
            init_tracing(&db_cmd.log_level)?;
            match db_cmd.command {
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
                DbCommands::DexVolumes(args) => {
                    tracing::info!("Backfilling dex volumes. Limit: {:?}", args.limit);
                    allium::backfill_dex_volumes(args.api_key, args.query_id, Some(args.limit))
                        .await?;
                    tracing::info!("Dex volumes backfill complete");
                    return Ok(());
                }
            }
        }
        Commands::Stream(args) => {
            init_tracing(&args.log_level)?;
            run_stream(args).await
        }
    }
}

async fn run_stream(args: StreamArgs) -> eyre::Result<()> {
    // Create app configuration from stream args
    let app_config = AppConfig::from_stream_args(&args)?;

    let binance_symbols: Vec<String> = app_config.exchange_configs.binance_symbols.clone();
    let binance_futures_symbols: Vec<String> = app_config.exchange_configs.binance_futures_symbols.clone();
    let bybit_symbols: Vec<String> = app_config.exchange_configs.bybit_symbols.clone();
    let okx_symbols: Vec<String> = app_config.exchange_configs.okx_symbols.clone();
    let coinbase_symbols: Vec<String> = app_config.exchange_configs.coinbase_symbols.clone();
    let kraken_symbols: Vec<String> = app_config.exchange_configs.kraken_symbols.clone();
    let kucoin_symbols: Vec<String> = app_config.exchange_configs.kucoin_symbols.clone();

    // Check if any data producer tasks will be enabled
    let has_producers = app_config.has_exchange_symbols()
        || app_config.ethereum_config.enabled
        || app_config.timeboost_config.enabled
        || app_config.allium_config.enabled;

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
        let client = BinanceClient::builder()
            .add_symbols(binance_symbols)
            .build()?;
        task_manager.spawn_task(TaskName::BinanceStream, move || {
            let client = client.clone();
            let tx = tx.clone();
            Box::pin(async move { process_exchange_stream(client, tx).await.into_task_result() })
        });
    }

    if !binance_futures_symbols.is_empty() {
        let tx = msg_tx.clone();
        let client = BinanceFuturesClient::builder()
            .add_symbols(binance_futures_symbols)
            .build()?;
        task_manager.spawn_task(TaskName::BinanceFuturesStream, move || {
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
        let client = CoinbaseClient::builder()
            .add_symbols(coinbase_symbols)
            .build()?;
        task_manager.spawn_task(TaskName::CoinbaseStream, move || {
            let client = client.clone();
            let tx = tx.clone();
            Box::pin(async move { process_exchange_stream(client, tx).await.into_task_result() })
        });
    }

    if !kraken_symbols.is_empty() {
        let tx = msg_tx.clone();
        let client = KrakenClient::builder()
            .add_symbols(kraken_symbols)
            .build()?;
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
            Box::pin(async move {
                ethereum::fetch_blocks_task(rpc_url, tx)
                    .await
                    .into_task_result()
            })
        });
    }

    if app_config.timeboost_config.enabled {
        tracing::info!("Spawning timeboost bids task");
        let tx = msg_tx.clone();
        task_manager.spawn_task(TaskName::TimeboostBids, move || {
            let tx = tx.clone();
            Box::pin(async move {
                timeboost::bids::fetch_bids_task(tx)
                    .await
                    .into_task_result()
            })
        });
    }

    if app_config.allium_config.enabled {
        tracing::info!("Spawning allium dex volumes task");
        let config = app_config.allium_config.clone();
        let tx = msg_tx.clone();
        task_manager.spawn_task(TaskName::AlliumDexVolumes, move || {
            let tx = tx.clone();
            let config = config.clone();
            Box::pin(async move {
                allium::fetch_dex_volumes_task(config, tx)
                    .await
                    .into_task_result()
            })
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
            Box::pin(async move {
                clickhouse_writer_task(rx, rate_limit, batch_size)
                    .await
                    .into_task_result()
            })
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

    pin_mut!(combined_stream);
    while let Some(event_result) = combined_stream.next().await {
        match event_result {
            Ok(event) => {
                let msg = ClickhouseMessage::Cex(event);
                if evt_tx.send(msg).is_err() {
                    tracing::error!("Failed to send message to channel");
                    return Err(eyre::eyre!("Channel send failed"));
                }
            }
            Err(e) => {
                tracing::error!("Error streaming {} event: {:?}", exchange_name, e);
                return Err(eyre::eyre!("Stream error: {}", e));
            }
        }
    }
    tracing::warn!("{} stream ended unexpectedly", exchange_name);
    Err(eyre::eyre!("{} stream terminated", exchange_name))
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
