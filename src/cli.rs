use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "exc-clickhouse")]
#[command(about = "Exchange data collector to ClickHouse database")]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// One-time database tasks
    Db(DbCmd),

    /// Run streaming tasks
    Stream(StreamArgs),
}

#[derive(Subcommand, Clone)]
pub enum DbCommands {
    /// Backfill timeboost bids
    Timeboost,
    /// Backfill trading pairs
    TradingPairs(TradingPairsArgs),
    /// Backfill dex volumes
    DexVolumes(DexVolumesArgs),
}

#[derive(Args, Clone)]
pub struct TradingPairsArgs {
    /// Path to trading pairs file
    #[arg(short, long, default_value = "trading_pairs.yaml")]
    pub trading_pairs_file: String,
}

#[derive(Args, Clone)]
pub struct DexVolumesArgs {
    #[arg(short, long, default_value_t = 10_000)]
    pub limit: usize,

    #[arg(long, env = "ALLIUM_API_KEY")]
    pub api_key: String,

    #[arg(long, env = "ALLIUM_DEX_VOLUME_QUERY_ID")]
    pub query_id: String,
}

#[derive(Args, Clone)]
pub struct DbCmd {
    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    pub log_level: String,

    #[command(subcommand)]
    pub command: DbCommands,
}

#[derive(Args, Clone)]
pub struct StreamArgs {
    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    pub log_level: String,

    /// Path to trading pairs file
    #[arg(short, long, default_value = "trading_pairs.yaml")]
    pub trading_pairs_file: String,

    /// Batch size for processing events
    #[arg(short, long, default_value_t = 500)]
    pub batch_size: usize,
    /// Skip Ethereum block metadata
    #[arg(long)]
    pub skip_ethereum: bool,

    /// Skip Timeboost bids
    #[arg(long)]
    pub skip_timeboost: bool,

    /// Skip Allium dex volumes
    #[arg(long)]
    pub skip_allium: bool,

    /// RPC URL for Ethereum (overrides environment variable)
    #[arg(long)]
    pub rpc_url: Option<String>,

    /// Enable automatic restart of failed tasks
    #[arg(long)]
    pub enable_restart: bool,

    /// Maximum number of restart attempts per task (0 = unlimited)
    #[arg(long, default_value_t = 0)]
    pub max_restart_attempts: u32,

    /// Initial restart delay in seconds
    #[arg(long, default_value_t = 1)]
    pub restart_delay_seconds: u64,

    /// Maximum restart delay in seconds (for exponential backoff)
    #[arg(long, default_value_t = 300)]
    pub max_restart_delay_seconds: u64,

    /// Rate limit for ClickHouse requests per second
    #[arg(long, default_value_t = 5)]
    pub clickhouse_rate_limit: u64,

    #[arg(long, env = "ALLIUM_API_KEY")]
    pub allium_api_key: Option<String>,

    #[arg(long, env = "ALLIUM_DEX_VOLUME_QUERY_ID")]
    pub allium_query_id: Option<String>,
}
