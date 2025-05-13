use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use clickhouse::Row;
use futures::StreamExt;
use serde::{Serialize, Deserialize};
use crate::clickhouse::{ClickHouseConfig, ClickHouseService};

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct BlockMetadata {
  // TODO: add more block metadata if possible.
  block_number: u64,
  block_hash: String,
  block_timestamp: u64,
  valid: bool,
}

pub async fn block_metadata_task(rpc_url: String) -> eyre::Result<()> {
  let ws = WsConnect::new(rpc_url);
  let provider = ProviderBuilder::new().connect_ws(ws).await?;
  let block_stream = provider.subscribe_blocks().await?.into_stream();
  let clickhouse = ClickHouseService::new(ClickHouseConfig::from_env()?);
  let metadata_stream = block_stream.map(|block| {
    BlockMetadata {
      block_number: block.number,
      block_hash: block.hash.to_string(),
      block_timestamp: block.timestamp,
      // FIXME: this is probably for handling uncles, which may complicate our insertion logic quite a bit. 
      valid: true,
    }
  });
  let count = clickhouse.write_block_metadata_stream(metadata_stream).await?;
  tracing::info!("Wrote {} block metadata to clickhouse", count.rows);
  Ok(())
} 