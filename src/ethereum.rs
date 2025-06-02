use crate::clickhouse::{ClickHouseConfig, ClickHouseService};
use crate::tower_utils::DelayLayer;
use alloy::consensus::BlockHeader;
use alloy::eips::BlockNumberOrTag;
use alloy::network::Ethereum;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::client::RpcClient;
use alloy::transports::layers::RetryBackoffLayer;
use async_stream::try_stream;
use clickhouse::Row;
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use url::Url;

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct BlockMetadata {
    // TODO: add more block metadata if possible.
    block_number: u64,
    block_hash: String,
    block_timestamp: u64,
    valid: bool,
}

pub struct BlockMetadataFetcher {
    provider: Box<dyn Provider<Ethereum>>,
}

impl BlockMetadataFetcher {
    pub fn new(rpc_url: String) -> Self {
        let retry_layer = RetryBackoffLayer::new(10, 1000, 1000);
        let delay_layer = DelayLayer::new(Duration::from_millis(50));
        let client = RpcClient::builder()
            .layer(retry_layer)
            .layer(delay_layer)
            .http(Url::parse(rpc_url.as_str()).unwrap());
        let provider = ProviderBuilder::new().connect_client(client);
        let boxed = Box::new(provider);
        Self { provider: boxed }
    }

    pub async fn stream_blocks(
        &self,
    ) -> eyre::Result<impl Stream<Item = eyre::Result<BlockMetadata>>> {
        let latest_block = self
            .provider
            .get_block_by_number(BlockNumberOrTag::Latest)
            .await?;
        let latest_block_number = latest_block
            .ok_or(eyre::eyre!("Failed to get latest block"))?
            .header
            .number();
        let block_stream = self.create_block_metadata_stream(latest_block_number).await;
        Ok(block_stream)
    }

    pub async fn create_block_metadata_stream(
        &self,
        start_block_number: u64,
    ) -> impl Stream<Item = eyre::Result<BlockMetadata>> {
        try_stream! {
          let mut current_block_number = start_block_number;
          loop {
            let block = self.provider.get_block_by_number(BlockNumberOrTag::Number(current_block_number)).await?;
            if let Some(block) = block {
              tracing::info!("Fetched block: {}", block.header.number());
              let block_metadata = BlockMetadata {
                block_number: block.header.number(),
                block_hash: block.header.hash.to_string(),
                block_timestamp: block.header.timestamp,
                valid: true,
              };
              yield block_metadata;
              current_block_number += 1;
            }
          }
        }
    }
}

pub async fn stream_blocks_to_clickhouse(rpc_url: String) -> eyre::Result<()> {
    let fetcher = BlockMetadataFetcher::new(rpc_url);
    let block_stream = fetcher.stream_blocks().await?;
    let clickhouse_service = ClickHouseService::new(ClickHouseConfig::from_env()?);
    clickhouse_service
        .write_block_metadata_stream(block_stream)
        .await?;
    Ok(())
}
