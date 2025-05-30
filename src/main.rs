use dotenv::dotenv;
use eyre::WrapErr;
use futures::stream::StreamExt;
use std::fs::File;
use std::io::{self, BufRead};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tower::ServiceExt;
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use clap::Parser;

use crate::{
    clickhouse::{ClickHouseConfig, ClickHouseService},
    models::NormalizedEvent,
    streams::{CombinedStream, ExchangeStreamError, binance::BinanceClient},
};

mod clickhouse;
mod models;
mod streams;
mod ethereum;
mod timeboost;

#[derive(Parser)]
#[command(name = "exc-clickhouse")]
#[command(about = "Exchange data collector to ClickHouse database")]
#[command(version)]
struct Cli {
    /// Path to symbols file
    #[arg(short, long, default_value = "symbols.txt")]
    symbols_file: String,

    /// Batch size for processing events
    #[arg(short, long, default_value_t = 500)]
    batch_size: usize,

    #[arg(long)]
    insert_all_timeboost_bids: bool,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: String,

    /// Skip Binance stream
    #[arg(long)]
    skip_binance: bool,

    /// Skip Ethereum block metadata
    #[arg(long)]
    skip_ethereum: bool,

    /// Skip ClickHouse writer
    #[arg(long)]
    skip_clickhouse: bool,

    /// Skip Timeboost bids
    #[arg(long)]
    skip_timeboost: bool,

    /// RPC URL for Ethereum (overrides environment variable)
    #[arg(long)]
    rpc_url: Option<String>,
}

fn read_symbols(filename: &str) -> eyre::Result<Vec<String>> {
    let file = File::open(filename).wrap_err("Failed to open symbols.txt")?;
    let reader = io::BufReader::new(file);
    reader
        .lines()
        .enumerate()
        .map(|(idx, line)| {
            line.wrap_err(format!("Failed to read line {}", idx + 1))
                .and_then(|line| {
                    if line.is_empty() {
                        Err(eyre::eyre!("Empty line at {}", idx + 1))
                    } else {
                        Ok(line.trim().to_lowercase())
                    }
                })
        })
        .collect::<eyre::Result<Vec<String>>>()
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
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(&log_level))
        )
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let (evt_tx, evt_rx) =
        mpsc::channel::<Vec<Result<NormalizedEvent, ExchangeStreamError>>>(50000);


    if cli.insert_all_timeboost_bids {
        tracing::info!("Inserting all timeboost bids");
        timeboost::bids::insert_all_timeboost_bids().await?;
        tracing::info!("All timeboost bids inserted");
        return Ok(());
    }

    let mut set = tokio::task::JoinSet::new();

    // Spawn Binance stream task if not skipped
    if !cli.skip_binance {
        let symbols = read_symbols(&cli.symbols_file)?;
        let batch_size = cli.batch_size;

        tracing::info!("Spawning binance stream for symbols: {:?}", symbols);
        set.spawn(async move{
            match binance_stream_task(evt_tx.clone(), symbols, batch_size).await {
                Ok(()) => Ok(()),
                Err(e) => {
                    tracing::error!("Binance stream task failed: {}", e);
                    Err(e)
                }
            }
        });
    }

    // Spawn Ethereum block metadata task if not skipped
    if !cli.skip_ethereum {
        let rpc_url = cli.rpc_url.or_else(|| std::env::var("RPC_URL").ok())
            .ok_or_else(|| eyre::eyre!("RPC_URL must be provided via --rpc-url flag or RPC_URL environment variable"))?;
        
        tracing::info!("Spawning ethereum block metadata task from RPC URL: {}", rpc_url);
        set.spawn(async {
            match ethereum::block_metadata_task(rpc_url).await {
                Ok(()) => Ok(()),
                Err(e) => {
                    tracing::error!("Ethereum block metadata task failed: {}", e);
                    Err(e)
                }
            }
        });
    }

    // Spawn ClickHouse writer task if not skipped
    if !cli.skip_clickhouse {
        tracing::info!("Spawning clickhouse writer task");
        set.spawn(async {
            match clickhouse_cex_writer_task(evt_rx).await {
                Ok(()) => Ok(()),
                Err(e) => {
                    tracing::error!("Clickhouse writer task failed: {}", e);
                    Err(e)
                }
            }
        });
    }

    // Spawn Timeboost bids task if not skipped
    if !cli.skip_timeboost {
        tracing::info!("Spawning timeboost bids task");
        set.spawn(async {
            match timeboost::bids::insert_timeboost_bids_task().await {
                Ok(()) => Ok(()),
                Err(e) => {
                    tracing::error!("Timeboost bids task failed: {}", e);
                    Err(e)
                }
            }
        });
    }

    // If no tasks were spawned, exit early
    if set.is_empty() {
        tracing::warn!("No tasks were spawned. Use --help to see available options.");
        return Ok(());
    }

    tokio::select! {
        _ = async {
            while let Some(res) = set.join_next().await {
                match res {
                    Ok(Ok(())) => {
                        // one worker returned Ok(())
                        continue;
                    }
                    Ok(Err(err)) => {
                        tracing::error!("worker task failed: {:?}", err);
                        break;
                    }
                    Err(join_err) => {
                        tracing::error!("worker task panicked: {}", join_err);
                        break;
                    }
                }
            }
        } => {
            tracing::info!("One of the worker tasks has finished; shutting down remaining tasks");
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("SIGINT received; shutting down worker tasks");
        }
    }

    // abort any remaining in-flight tasks
    set.abort_all();

    // Wait for all tasks to complete their abort
    while let Some(res) = set.join_next().await {
        if let Err(e) = res {
            tracing::warn!("Error during task shutdown: {}", e);
        }
    }
    tracing::info!("All tasks have been shut down");
    Ok(())
}

async fn binance_stream_task(
    evt_tx: mpsc::Sender<Vec<Result<NormalizedEvent, ExchangeStreamError>>>,
    symbols: Vec<String>,
    batch_size: usize,
) -> eyre::Result<()> {
    let binance = BinanceClient::builder()
        .add_symbols(symbols)
        .with_quotes(true)
        .with_trades(true)
        .build()?;
    let combined_stream = binance.combined_stream().await?;
    let chunks = combined_stream.chunks(batch_size);

    chunks
        .for_each_concurrent(10, |batch| {
            let tx = evt_tx.clone();
            async move {
                if let Err(e) = tx.send(batch).await {
                    tracing::error!("Error sending batch to ClickHouse: {:?}", e);
                }
            }
        })
        .await;
    Ok(())
}

async fn clickhouse_cex_writer_task(
    rx: mpsc::Receiver<Vec<Result<NormalizedEvent, ExchangeStreamError>>>,
) -> eyre::Result<()> {
    let cfg = ClickHouseConfig::from_env()?;
    let writer = ClickHouseService::new(cfg);

    let batch_stream = ReceiverStream::new(rx)
        .map(|batch| {
            batch
                .into_iter()
                .inspect(|result| {
                    if let Err(e) = result {
                        tracing::error!("{:?}", e);
                    }
                })
                .filter_map(Result::ok)
                .collect::<Vec<_>>()
        })
        .filter(|events| futures::future::ready(!events.is_empty()));

    let mut resp_stream = writer.call_all(batch_stream);
    while let Some(result) = resp_stream.next().await {
        if let Err(e) = result {
            tracing::error!("Error writing to ClickHouse: {:?}", e);
        }
    }
    Ok(())
}
