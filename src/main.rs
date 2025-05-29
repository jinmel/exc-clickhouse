use dotenv::dotenv;
use eyre::WrapErr;
use futures::stream::StreamExt;
use std::fs::File;
use std::io::{self, BufRead};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tower::ServiceExt;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

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

const BATCH_SIZE: usize = 500;

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
    // Load environment variables from .env file
    dotenv().ok();

    // Initialize tracing with environment filter
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info"))
        )
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let (evt_tx, evt_rx) =
        mpsc::channel::<Vec<Result<NormalizedEvent, ExchangeStreamError>>>(50000);

    let mut set = tokio::task::JoinSet::new();

    let symbols = read_symbols("symbols.txt")?;

    tracing::info!("Spawning binance stream for symbols: {:?}", symbols);
    set.spawn(async move{
        match binance_stream_task(evt_tx.clone(), symbols).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::error!("Binance stream task failed: {}", e);
                Err(e)
            }
        }
    });

    let rpc_url = std::env::var("RPC_URL").map_err(|_| {
        eyre::eyre!("RPC_URL environment variable is not set")
    })?;
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
) -> eyre::Result<()> {
    let binance = BinanceClient::builder()
        .add_symbols(symbols)
        .with_quotes(true)
        .with_trades(true)
        .build()?;
    let combined_stream = binance.combined_stream().await?;
    let chunks = combined_stream.chunks(BATCH_SIZE);

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
