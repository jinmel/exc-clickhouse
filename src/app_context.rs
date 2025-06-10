use eyre::Result;
use tokio::sync::mpsc;

use crate::{
    TaskName,
    clickhouse::clickhouse_writer_task,
    config::{AppConfig, StreamArgs},
    ethereum,
    models::ClickhouseMessage,
    streams::{
        binance::BinanceClient, bybit::BybitClient, coinbase::CoinbaseClient, kraken::KrakenClient,
        kucoin::KucoinClient, okx::OkxClient, process_exchange_stream,
    },
    task_manager::{IntoTaskResult, TaskManager, TaskManagerConfig},
    timeboost,
};

/// Central application context that holds all shared resources
pub struct AppContext {
    pub config: AppConfig,
    pub task_manager: TaskManager<()>,
    pub message_sender: mpsc::UnboundedSender<ClickhouseMessage>,
    pub message_receiver: Option<mpsc::UnboundedReceiver<ClickhouseMessage>>,
    pub has_producers: bool,
}

impl AppContext {
    /// Run the application with proper task management and shutdown handling
    pub async fn run(mut self) -> Result<()> {
        // Spawn tasks based on configuration
        self.spawn_tasks().await?;

        // Drop the sender to ensure proper cleanup
        drop(self.message_sender);

        // Monitor tasks using TaskManager with restart capabilities
        tokio::select! {
            result = self.task_manager.run() => {
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
                if let Err(e) = self.task_manager.shutdown().await {
                    tracing::error!("Error during TaskManager shutdown: {:?}", e);
                }
            }
        }

        tracing::info!("Application shutdown complete");
        Ok(())
    }

    /// Spawn all enabled tasks based on configuration
    async fn spawn_tasks(&mut self) -> Result<()> {
        let msg_tx = self.message_sender.clone();

        // Spawn exchange streaming tasks
        if !self.config.exchange_configs.binance_symbols.is_empty() {
            let symbols = self.config.exchange_configs.binance_symbols.clone();
            let tx = msg_tx.clone();
            tracing::info!("Spawning binance stream for symbols: {:?}", symbols);
            self.task_manager
                .spawn_task(TaskName::BinanceStream, move || async move {
                    let client = BinanceClient::builder().add_symbols(symbols).build()?;
                    process_exchange_stream(client, tx).await.into_task_result()
                });
        }

        if !self.config.exchange_configs.bybit_symbols.is_empty() {
            let symbols = self.config.exchange_configs.bybit_symbols.clone();
            let tx = msg_tx.clone();
            tracing::info!("Spawning bybit stream for symbols: {:?}", symbols);
            self.task_manager
                .spawn_task(TaskName::BybitStream, move || async move {
                    let client = BybitClient::builder().add_symbols(symbols).build()?;
                    process_exchange_stream(client, tx).await.into_task_result()
                });
        }

        if !self.config.exchange_configs.okx_symbols.is_empty() {
            let symbols = self.config.exchange_configs.okx_symbols.clone();
            let tx = msg_tx.clone();
            tracing::info!("Spawning okx stream for symbols: {:?}", symbols);
            self.task_manager
                .spawn_task(TaskName::OkxStream, move || async move {
                    let client = OkxClient::builder().add_symbols(symbols).build()?;
                    process_exchange_stream(client, tx).await.into_task_result()
                });
        }

        if !self.config.exchange_configs.coinbase_symbols.is_empty() {
            let symbols = self.config.exchange_configs.coinbase_symbols.clone();
            let tx = msg_tx.clone();
            tracing::info!("Spawning coinbase stream for symbols: {:?}", symbols);
            self.task_manager
                .spawn_task(TaskName::CoinbaseStream, move || async move {
                    let client = CoinbaseClient::builder().add_symbols(symbols).build()?;
                    process_exchange_stream(client, tx).await.into_task_result()
                });
        }

        if !self.config.exchange_configs.kraken_symbols.is_empty() {
            let symbols = self.config.exchange_configs.kraken_symbols.clone();
            let tx = msg_tx.clone();
            tracing::info!("Spawning kraken stream for symbols: {:?}", symbols);
            self.task_manager
                .spawn_task(TaskName::KrakenStream, move || async move {
                    let client = KrakenClient::builder().add_symbols(symbols).build()?;
                    process_exchange_stream(client, tx).await.into_task_result()
                });
        }

        if !self.config.exchange_configs.kucoin_symbols.is_empty() {
            let symbols = self.config.exchange_configs.kucoin_symbols.clone();
            let tx = msg_tx.clone();
            tracing::info!("Spawning kucoin stream for symbols: {:?}", symbols);
            self.task_manager
                .spawn_task(TaskName::KucoinStream, move || async move {
                    let client = KucoinClient::builder().add_symbols(symbols).build().await?;
                    process_exchange_stream(client, tx).await.into_task_result()
                });
        }

        // Spawn Ethereum task
        if self.config.ethereum_config.enabled {
            let rpc_url = self.config.get_rpc_url()?;
            let tx = msg_tx.clone();
            tracing::info!(
                "Spawning ethereum block metadata task from RPC URL: {}",
                rpc_url
            );
            self.task_manager
                .spawn_task(TaskName::EthereumBlockMetadata, move || async move {
                    ethereum::fetch_blocks_task(rpc_url, tx)
                        .await
                        .into_task_result()
                });
        }

        // Spawn Timeboost task
        if self.config.timeboost_config.enabled {
            let tx = msg_tx.clone();
            tracing::info!("Spawning timeboost bids task");
            self.task_manager
                .spawn_task(TaskName::TimeboostBids, move || async move {
                    timeboost::bids::fetch_bids_task(tx)
                        .await
                        .into_task_result()
                });
        }

        // Automatically spawn ClickHouse writer if we have any data producers
        if self.has_producers {
            let msg_rx = self.message_receiver
                .take()
                .expect("ClickHouse receiver should be available");
            let rate_limit = self.config.clickhouse_rate_limit;
            let batch_size = self.config.batch_size;

            tracing::info!("Spawning clickhouse writer task (auto-enabled for producer tasks)");
            self.task_manager
                .spawn_task(TaskName::ClickHouseWriter, move || async move {
                    clickhouse_writer_task(msg_rx, rate_limit, batch_size)
                        .await
                        .into_task_result()
                });
        }

        Ok(())
    }
}

/// Builder pattern for fluent AppContext configuration
#[derive(Debug, Default)]
pub struct AppBuilder {
    config: Option<AppConfig>,
    task_manager_config: Option<TaskManagerConfig>,
}

impl AppBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the application configuration
    pub fn with_config(mut self, config: AppConfig) -> Self {
        self.config = Some(config.clone());
        self.task_manager_config = Some(config.get_task_manager_config());
        self
    }

    /// Set the task manager configuration
    pub fn with_task_manager_config(mut self, task_config: TaskManagerConfig) -> Self {
        self.task_manager_config = Some(task_config);
        self
    }

    /// Build from StreamArgs (convenience method)
    pub fn from_stream_args(args: &StreamArgs, cli_log_level: &str) -> Result<Self> {
        let config = AppConfig::from_stream_args(args, cli_log_level)?;
        let task_manager_config = config.get_task_manager_config();

        Ok(Self::new()
            .with_config(config)
            .with_task_manager_config(task_manager_config))
    }

    /// Build the AppContext with validation
    pub fn build(self) -> Result<AppContext> {
        let config = self
            .config
            .ok_or_else(|| eyre::eyre!("AppConfig is required"))?;

        // Check if any data producer tasks will be enabled
        let has_producers = config.has_exchange_symbols()
            || config.ethereum_config.enabled
            || config.timeboost_config.enabled;

        if !has_producers {
            return Err(eyre::eyre!(
                "No tasks were enabled. Use --help to see available options."
            ));
        }

        // Create TaskManager with configuration from AppConfig or override
        let task_config = self
            .task_manager_config
            .unwrap_or_else(|| config.get_task_manager_config());
        let task_manager = TaskManager::<()>::with_config(task_config);

        // Create communication channel
        let (message_sender, message_receiver) = mpsc::unbounded_channel::<ClickhouseMessage>();

        Ok(AppContext {
            config,
            task_manager,
            message_sender,
            message_receiver: Some(message_receiver),
            has_producers,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_builder_fluent_interface() {
        let config = AppConfig::default();

        let builder = AppBuilder::new().with_config(config);

        // Should not panic and builder should be configured
        assert!(builder.config.is_some());
        assert!(builder.task_manager_config.is_some());
    }

    #[test]
    fn test_app_builder_missing_config() {
        let builder = AppBuilder::new();
        let result = builder.build();
        assert!(result.is_err());
        let error_msg = result.err().unwrap().to_string();
        assert!(error_msg.contains("AppConfig is required"));
    }
}
