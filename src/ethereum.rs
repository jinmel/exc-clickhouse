use crate::clickhouse::{ClickHouseConfig, ClickHouseService};
use crate::tower_utils::DelayLayer;
use alloy::providers::DynProvider;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::client::RpcClient;
use alloy::transports::layers::RetryBackoffLayer;
use alloy::transports::ws::WsConnect;
use async_stream::try_stream;
use clickhouse::Row;
use futures::{Stream, pin_mut};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct BlockMetadata {
    // TODO: add more block metadata if possible.
    block_number: u64,
    block_hash: String,
    block_timestamp: u64,
    valid: bool,
}

pub struct BlockMetadataFetcher {
    provider: DynProvider,
}

impl BlockMetadataFetcher {
    pub async fn new(rpc_url: String) -> eyre::Result<Self> {
        let retry_layer = RetryBackoffLayer::new(10, 1000, 100);
        let delay_layer = DelayLayer::new(Duration::from_millis(50));
        let client = RpcClient::builder()
            .layer(retry_layer)
            .layer(delay_layer)
            .ws(WsConnect::new(rpc_url))
            .await?;
        let provider = ProviderBuilder::new().connect_client(client).erased();
        Ok(Self { provider })
    }

    pub async fn create_block_metadata_stream(
        &self,
    ) -> impl Stream<Item = eyre::Result<BlockMetadata>> {
        try_stream! {
          loop {
            let sub = self.provider.subscribe_blocks().await?;
            pin_mut!(sub);
            while let Ok(block) = sub.recv().await {
              let block_metadata = BlockMetadata {
                block_number: block.number,
                block_hash: block.hash.to_string(),
                block_timestamp: block.timestamp,
                valid: true,
              };
              tracing::info!("Fetched block: {}", block.number);
              yield block_metadata;
            }
            tracing::info!("Block stream ended. reconnecting...");
          }
        }
    }
}

pub async fn stream_blocks_to_clickhouse(rpc_url: String) -> eyre::Result<()> {
    let fetcher = BlockMetadataFetcher::new(rpc_url).await?;
    let block_stream = fetcher.create_block_metadata_stream().await;
    let clickhouse_service = ClickHouseService::new(ClickHouseConfig::from_env()?);
    clickhouse_service
        .write_block_metadata_stream(block_stream)
        .await?;
    Ok(())
}
