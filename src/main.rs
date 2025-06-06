use crate::symbols::{SymbolsConfig, SymbolsConfigEntry, fetch_binance_spot_symbols};
use clap::{Args, Parser, Subcommand};
use dotenv::dotenv;
use eyre::WrapErr;
use futures::pin_mut;
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::fs::File;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

use crate::{
    clickhouse::{ClickHouseConfig, ClickHouseService},
    models::{ClickhouseMessage, NormalizedEvent},
    streams::{
        ExchangeStreamError, WebsocketStream,
        binance::{BinanceClient, MARKET_ONLY_BINANCE_WS_URL},
    },
};

mod clickhouse;
mod ethereum;
mod models;
mod streams;
mod symbols;
mod timeboost;
mod tower_utils;

#[derive(Parser)]
#[command(name = "exc-clickhouse")]
#[command(about = "Exchange data collector to ClickHouse database")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: String,
}

#[derive(Subcommand)]
enum Commands {
    /// One-time database tasks
    Db(DbCmd),

    /// Run streaming tasks
    Stream(StreamArgs),
}

#[derive(Subcommand, Clone)]
enum DbCommands {
    /// Backfill timeboost bids
    Timeboost,
    /// Fetch top Binance SPOT symbols by volume
    FetchBinanceSymbols(FetchBinanceSymbolsArgs),
}

#[derive(Args, Clone)]
struct DbCmd {
    #[command(subcommand)]
    command: DbCommands,
}

#[derive(Args, Clone)]
struct FetchBinanceSymbolsArgs {
    /// Output YAML file path
    #[arg(short, long)]
    output: String,
}

#[derive(Args, Clone)]
struct StreamArgs {
    /// Path to symbols file
    #[arg(short, long, default_value = "symbols.yaml")]
    symbols_file: String,

    /// Batch size for processing events
    #[arg(short, long, default_value_t = 500)]
    batch_size: usize,

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

    /// Enable automatic restart of failed tasks
    #[arg(long)]
    enable_restart: bool,

    /// Maximum number of restart attempts per task (0 = unlimited)
    #[arg(long, default_value_t = 0)]
    max_restart_attempts: u32,

    /// Initial restart delay in seconds
    #[arg(long, default_value_t = 1)]
    restart_delay_seconds: u64,

    /// Maximum restart delay in seconds (for exponential backoff)
    #[arg(long, default_value_t = 300)]
    max_restart_delay_seconds: u64,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
enum TaskType {
    BinanceStream,
    EthereumBlockMetadata,
    ClickHouseInsert,
    FetchTimeboostBids,
}

impl std::fmt::Display for TaskType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskType::BinanceStream => write!(f, "Binance"),
            TaskType::EthereumBlockMetadata => write!(f, "Ethereum"),
            TaskType::ClickHouseInsert => write!(f, "ClickHouse"),
            TaskType::FetchTimeboostBids => write!(f, "Timeboost"),
        }
    }
}

struct TaskSupervisor {
    restart_attempts: HashMap<TaskType, u32>,
    max_attempts: u32,
    initial_delay: Duration,
    max_delay: Duration,
}

impl TaskSupervisor {
    fn new(max_attempts: u32, initial_delay_secs: u64, max_delay_secs: u64) -> Self {
        Self {
            restart_attempts: HashMap::new(),
            max_attempts,
            initial_delay: Duration::from_secs(initial_delay_secs),
            max_delay: Duration::from_secs(max_delay_secs),
        }
    }

    fn should_restart(&mut self, task_type: &TaskType) -> bool {
        if self.max_attempts == 0 {
            return true; // Unlimited restarts
        }

        let attempts = self.restart_attempts.entry(task_type.clone()).or_insert(0);
        *attempts < self.max_attempts
    }

    async fn wait_before_restart(&mut self, task_type: &TaskType) {
        let attempts = self.restart_attempts.entry(task_type.clone()).or_insert(0);
        *attempts += 1;

        // Exponential backoff: delay = min(initial_delay * 2^(attempts-1), max_delay)
        let delay = std::cmp::min(
            self.initial_delay * 2_u32.pow(attempts.saturating_sub(1)),
            self.max_delay,
        );

        tracing::info!(
            "Waiting {:?} before restarting {} task (attempt {})",
            delay,
            task_type,
            attempts
        );
        tokio::time::sleep(delay).await;
    }

    fn reset_attempts(&mut self, task_type: &TaskType) {
        self.restart_attempts.remove(task_type);
    }
}

fn read_symbols(filename: &str) -> eyre::Result<SymbolsConfig> {
    let file = File::open(filename).wrap_err("Failed to open symbols YAML file")?;
    SymbolsConfig::from_yaml(file)
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
                tracing::info!("Fetching top Binance SPOT symbols by volume");
                let symbols = fetch_binance_spot_symbols().await?;
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
                return Ok(());
            }
        },
        Commands::Stream(args) => run_stream(args).await,
    }
}

async fn run_stream(args: StreamArgs) -> eyre::Result<()> {
    // Initialize task supervisor if restart is enabled
    let mut supervisor = if args.enable_restart {
        Some(TaskSupervisor::new(
            args.max_restart_attempts,
            args.restart_delay_seconds,
            args.max_restart_delay_seconds,
        ))
    } else {
        None
    };

    if args.skip_binance && args.skip_ethereum && args.skip_clickhouse && args.skip_timeboost {
        tracing::warn!("No tasks were enabled. Use --help to see available options.");
        return Ok(());
    }

    loop {
        // Create new channel for each restart cycle
        let (msg_tx, msg_rx) = mpsc::unbounded_channel::<Vec<ClickhouseMessage>>();
        let mut set = tokio::task::JoinSet::new();

        // Spawn tasks
        if !args.skip_binance {
            let config = read_symbols(&args.symbols_file)?;
            let symbols: Vec<String> = config
                .entries
                .iter()
                .filter(|e| e.exchange.eq_ignore_ascii_case("binance"))
                .flat_map(|e| e.symbols.iter().cloned().map(|s| s.to_lowercase()))
                .collect();
            let batch_size = args.batch_size;
            let tx = msg_tx.clone();

            tracing::info!("Spawning binance stream for symbols: {:?}", symbols);
            set.spawn(async move {
                (
                    TaskType::BinanceStream,
                    binance_stream_task(tx, symbols, batch_size).await,
                )
            });
        }

        if !args.skip_ethereum {
            let rpc_url = args
                .rpc_url
                .clone()
                .or_else(|| std::env::var("RPC_URL").ok())
                .ok_or(eyre::eyre!(
                    "RPC_URL must be provided via --rpc-url flag or RPC_URL environment variable"
                ))?;

            tracing::info!(
                "Spawning ethereum block metadata task from RPC URL: {}",
                rpc_url
            );
            let tx = msg_tx.clone();
            set.spawn(async move {
                (
                    TaskType::EthereumBlockMetadata,
                    ethereum::fetch_blocks_task(rpc_url, tx).await,
                )
            });
        }

        if !args.skip_clickhouse {
            tracing::info!("Spawning clickhouse writer task");
            set.spawn(async move {
                (
                    TaskType::ClickHouseInsert,
                    clickhouse_writer_task(msg_rx).await,
                )
            });
        }

        if !args.skip_timeboost {
            tracing::info!("Spawning timeboost bids task");
            let tx = msg_tx.clone();
            set.spawn(async move {
                (
                    TaskType::FetchTimeboostBids,
                    timeboost::bids::fetch_bids_task(tx).await,
                )
            });
        }

        // Drop the sender to ensure proper cleanup
        drop(msg_tx);

        let mut should_restart = false;
        let mut failed_tasks = Vec::new();

        // Monitor tasks
        tokio::select! {
            _ = async {
                while let Some(res) = set.join_next().await {
                    match res {
                        Ok((task_type, Ok(()))) => {
                            tracing::info!("{} task completed successfully", task_type);
                            if let Some(ref mut supervisor) = supervisor {
                                supervisor.reset_attempts(&task_type);
                            }
                        }
                        Ok((task_type, Err(err))) => {
                            tracing::error!("{} task failed: {:?}", task_type, err);

                            if let Some(ref mut supervisor) = supervisor {
                                if supervisor.should_restart(&task_type) {
                                    failed_tasks.push(task_type.clone());
                                    should_restart = true;
                                    break;
                                } else {
                                    tracing::error!("{} task exceeded maximum restart attempts", task_type);
                                    break;
                                }
                            } else {
                                tracing::info!("Task restart is disabled, shutting down");
                                break;
                            }
                        }
                        Err(join_err) => {
                            tracing::error!("Task panicked: {}", join_err);
                            if supervisor.is_none() {
                                break;
                            }
                            // For panics, we'll restart all tasks to be safe
                            should_restart = true;
                            break;
                        }
                    }
                }
            } => {
                if !should_restart {
                    tracing::info!("All tasks completed or failed without restart enabled");
                    break;
                }
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("SIGINT received; shutting down worker tasks");
                break;
            }
        }

        // Abort remaining tasks
        set.abort_all();
        while let Some(res) = set.join_next().await {
            if let Err(e) = res {
                tracing::debug!("Error during task shutdown: {}", e);
            }
        }

        if !should_restart {
            break;
        }

        // Wait before restarting failed tasks
        if let Some(ref mut supervisor) = supervisor {
            for task_type in &failed_tasks {
                supervisor.wait_before_restart(task_type).await;
            }
            tracing::info!("Restarting failed tasks: {:?}", failed_tasks);
        }
    }

    tracing::info!("Application shutdown complete");
    Ok(())
}

async fn binance_stream_task(
    evt_tx: mpsc::UnboundedSender<Vec<ClickhouseMessage>>,
    symbols: Vec<String>,
    batch_size: usize,
) -> eyre::Result<()> {
    let binance = BinanceClient::builder().add_symbols(symbols).build()?;
    let combined_stream = binance.stream_events().await?;
    let chunks = combined_stream
        .filter_map(
            |event: Result<NormalizedEvent, ExchangeStreamError>| async move {
                match event {
                    Ok(NormalizedEvent::Trade(trade)) => {
                        Some(ClickhouseMessage::Cex(NormalizedEvent::Trade(trade)))
                    }
                    Ok(NormalizedEvent::Quote(quote)) => {
                        Some(ClickhouseMessage::Cex(NormalizedEvent::Quote(quote)))
                    }
                    Err(e) => {
                        // TODO: handle error
                        tracing::error!("Error parsing event: {:?}", e);
                        None
                    }
                }
            },
        )
        .chunks(batch_size);
    pin_mut!(chunks);
    while let Some(chunk) = chunks.next().await {
        let res = evt_tx.send(chunk);
        if res.is_err() {
            tracing::error!("Failed to send chunk to channel");
        }
    }
    Ok(())
}

async fn clickhouse_writer_task(
    rx: mpsc::UnboundedReceiver<Vec<ClickhouseMessage>>,
) -> eyre::Result<()> {
    let cfg = ClickHouseConfig::from_env()?;
    let clickhouse_svc = ClickHouseService::new(cfg);
    pin_mut!(rx);
    while let Some(batch) = rx.recv().await {
        clickhouse_svc.handle_msg(batch).await?;
    }
    Ok(())
}
