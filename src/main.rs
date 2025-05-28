use crate::models::ExchangeName;
use dotenv::dotenv;
use eyre::WrapErr;
use futures::stream::StreamExt;
use std::fs::File;
use std::io::{self, BufRead};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tower::ServiceExt;
use tracing_subscriber::{fmt, filter::EnvFilter};

use crate::clickhouse::Client;

use crate::{
    clickhouse::{ClickHouseConfig, ClickHouseService, ArbitrageData},
    models::NormalizedEvent,
    streams::upbit::UpbitClient,
    streams::{CombinedStream, ExchangeClient, ExchangeStreamError, binance::BinanceClient},
};

mod clickhouse;
mod ethereum;
mod models;
mod streams;

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

    // Initialize tracing
    let subscriber = fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info"))
        )
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let (evt_tx, evt_rx) = mpsc::channel::<Result<NormalizedEvent, ExchangeStreamError>>(50000);

    let mut set = tokio::task::JoinSet::new();

    //let symbols = read_symbols("symbols.txt")?;
    let symbols = vec!["CVCUSDT".to_string()];
    let upbit_symbols = vec!["KRW-CVC".to_string(), "KRW-USDT".to_string()];
    let evt_tx_for_upbit = evt_tx.clone();

    tracing::info!("Spawning binance stream for symbols: {:?}", symbols);
    set.spawn(async move {
        match binance_stream_task(evt_tx, symbols).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::error!("Binance stream task failed: {}", e);
                Err(e)
            }
        }
    });

    tracing::info!("Spawning upbit stream for symbols: {:?}", upbit_symbols);
    set.spawn(async move {
        match upbit_stream_task(evt_tx_for_upbit, upbit_symbols).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::error!("Upbit stream task failed: {}", e);
                Err(e)
            }
        }
    });

    tracing::info!("Spawning arbitrage task");
    set.spawn(async move {
        match arbitrage_task(evt_rx).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::error!("Arbitrage task failed: {}", e);
                Err(e)
            }
        }
    });

    // let rpc_url = std::env::var("RPC_URL")
    //     .map_err(|_| eyre::eyre!("RPC_URL environment variable is not set"))?;
    // tracing::info!(
    //     "Spawning ethereum block metadata task from RPC URL: {}",
    //     rpc_url
    // );
    // set.spawn(async {
    //     match ethereum::block_metadata_task(rpc_url).await {
    //         Ok(()) => Ok(()),
    //         Err(e) => {
    //             tracing::error!("Ethereum block metadata task failed: {}", e);
    //             Err(e)
    //         }
    //     }
    // });

    // set.spawn(async {
    //     match clickhouse_cex_writer_task(evt_rx).await {
    //         Ok(()) => Ok(()),
    //         Err(e) => {
    //             tracing::error!("Clickhouse writer task failed: {}", e);
    //             Err(e)
    //         }
    //     }
    // });

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
    evt_tx: mpsc::Sender<Result<NormalizedEvent, ExchangeStreamError>>,
    symbols: Vec<String>,
) -> eyre::Result<()> {
    let binance = BinanceClient::builder()
        .add_symbols(symbols)
        .with_quotes(true)
        .with_trades(false)
        .build()?;
    let mut combined_stream = binance.combined_stream().await?;
    while let Some(event) = combined_stream.next().await {
        if let Err(e) = evt_tx.send(event).await {
            tracing::error!("Error sending event to ClickHouse: {:?}", e);
        }
    }
    Ok(())
}

async fn upbit_stream_task(
    evt_tx: mpsc::Sender<Result<NormalizedEvent, ExchangeStreamError>>,
    symbols: Vec<String>,
) -> eyre::Result<()> {
    let upbit = UpbitClient::builder()
        .with_quotes(true)
        .add_symbols(symbols)
        .build()?;
    let quote_stream = upbit.normalized_quotes().await?;
    let mut events = quote_stream.map(|quote| {
        quote
            .map(|quote| NormalizedEvent::Quote(quote))
            .map_err(|e| ExchangeStreamError::StreamError(e.to_string()))
    });
    while let Some(event) = events.next().await {
        if let Err(e) = evt_tx.send(event).await {
            tracing::error!("Error sending event to ClickHouse: {:?}", e);
        }
    }
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

async fn arbitrage_task(
    mut rx: mpsc::Receiver<Result<NormalizedEvent, ExchangeStreamError>>,
) -> eyre::Result<()> {
    let mut binance_ask_price = 0.0;
    let mut binance_bid_price = 0.0;
    let mut upbit_ask_price = 0.0;
    let mut upbit_bid_price = 0.0;

    let mut upbit_usdt_ask_price = None;
    let mut upbit_usdt_bid_price = None;

    let client = Client::default()
        .with_url("http://matroos.xyz:8123")
        .with_user("brontes")
        .with_password("brontes");

    let mut inserter = client
        .inserter::<ArbitrageData>("default.arbitrage_task")?
        .with_max_rows(100)
        .with_period(Some(Duration::from_secs(1)))
        .with_period_bias(0.1);

    while let Some(event) = rx.recv().await {
        match event {
            Ok(event) => match event {
                NormalizedEvent::Quote(quote) => {
                    if quote.symbol.to_string() == "KRW-USDT" {
                        upbit_usdt_ask_price = Some(quote.ask_price);
                        upbit_usdt_bid_price = Some(quote.bid_price);
                        continue;
                    }

                    if quote.symbol.to_string() == "CVCUSDT"
                        || quote.symbol.to_string() == "KRW-CVC"
                    {
                        if quote.exchange == ExchangeName::Binance {
                            binance_ask_price = quote.ask_price;
                            binance_bid_price = quote.bid_price;
                            tracing::debug!("Received binance quote: {:?}", quote);
                        } else if quote.exchange == ExchangeName::Upbit {
                            upbit_ask_price = quote.ask_price;
                            upbit_bid_price = quote.bid_price;
                            tracing::debug!("Received upbit quote: {:?}", quote);
                        }
                        let timestamp = quote.timestamp;
                        if let (Some(upbit_usdt_ask_price), Some(upbit_usdt_bid_price)) =
                            (upbit_usdt_ask_price, upbit_usdt_bid_price)
                        {
                            let upbit_ask_price_usd = upbit_ask_price / upbit_usdt_ask_price;
                            let upbit_bid_price_usd = upbit_bid_price / upbit_usdt_ask_price;
                            
                            // Create arbitrage data record
                            let arbitrage_data = ArbitrageData {
                                timestamp,
                                binance_ask: binance_ask_price,
                                binance_bid: binance_bid_price,
                                upbit_ask: upbit_ask_price_usd,
                                upbit_bid: upbit_bid_price_usd,
                                upbit_usdt_ask: upbit_usdt_ask_price,
                                upbit_usdt_bid: upbit_usdt_bid_price,
                            };
                            
                            // Insert into ClickHouse
                            if let Err(e) = inserter.write(&arbitrage_data) {
                                tracing::error!("Failed to write arbitrage data to ClickHouse: {}", e);
                            }
                            
                            tracing::info!(
                                ?timestamp,
                                ?binance_ask_price,
                                ?binance_bid_price,
                                upbit_ask_price_usd = ?upbit_ask_price_usd,
                                upbit_bid_price_usd = ?upbit_bid_price_usd,
                                ?upbit_usdt_ask_price,
                                ?upbit_usdt_bid_price,
                            );

                            inserter.commit().await?;
                        }
                    }
                }
                _ => {}
            },
            Err(e) => {
                tracing::error!("Error receiving event: {:?}", e);
            }
        }
    }

    // Finalize the inserter when the task ends
    if let Err(e) = inserter.end().await {
        tracing::error!("Failed to finalize ClickHouse inserter: {}", e);
    }

    Ok(())
}
