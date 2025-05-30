use crate::clickhouse::{ClickHouseConfig, ClickHouseService};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use clickhouse::Row;
use csv;
use flate2::read::GzDecoder;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::task::{Context, Poll};
use tokio::time::Duration;
use tower::Service;
use tower::limit::RateLimitLayer;
use tower::timeout::TimeoutLayer;
use tower::util::ServiceExt;
use chrono::{DateTime, Utc};

fn default_timestamp() -> DateTime<Utc> {
    Utc::now()
}

// ClickHouse table schema for proper time filtering:
// CREATE TABLE timeboost.bids
// (
//     `timestamp` DateTime64(3),  -- DateTime with millisecond precision for $__timefilter
//     `chain_id` UInt64,
//     `bidder` String,
//     `express_lane_controller` String,
//     `auction_contract_address` String,
//     `round` UInt64,
//     `amount` String,
//     `signature` String
// )
// ENGINE = MergeTree
// PARTITION BY chain_id
// PRIMARY KEY round
// ORDER BY round

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3ObjectInfo {
    pub key: String,
    pub size: i64,
    pub last_modified: Option<String>,
    pub etag: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct BidData {
    #[serde(skip, default = "default_timestamp")]
    pub timestamp: DateTime<Utc>,
    #[serde(alias = "ChainID")]
    pub chain_id: u64,
    #[serde(alias = "Bidder")]
    pub bidder: String,
    #[serde(alias = "ExpressLaneController")]
    pub express_lane_controller: String,
    #[serde(alias = "AuctionContractAddress")]
    pub auction_contract_address: String,
    #[serde(alias = "Round")]
    pub round: u64,
    #[serde(alias = "Amount")]
    pub amount: String, // Keep as String to handle large numbers
    #[serde(alias = "Signature")]
    pub signature: String,
}

impl BidData {
    pub fn with_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.timestamp = timestamp;
        self
    }
    
    pub fn with_current_timestamp(mut self) -> Self {
        self.timestamp = Utc::now();
        self
    }
}

pub async fn insert_timeboost_bids_task() -> eyre::Result<()> {
    let inner = HistoricalBidsService::new().await?;
    let mut svc = tower::ServiceBuilder::new()
        .layer(TimeoutLayer::new(Duration::from_secs(10)))
        .layer(RateLimitLayer::new(1, Duration::from_secs(60)))
        .service(inner);

    let clickhouse = ClickHouseService::new(ClickHouseConfig::from_env()?);
    loop {
        tracing::trace!("Waiting for service to be ready");
        svc.ready()
            .await
            .map_err(|e| eyre::eyre!("Service not ready: {}", e))?;
        tracing::trace!("Service is ready");
        let mut bids = svc
            .call(())
            .await
            .map_err(|e| eyre::eyre!("Service call failed: {}", e))?;
        tracing::debug!("Got {} bids from s3", bids.len());
        if bids.is_empty() {
            tracing::warn!("No bids found from s3");
            continue;
        }

        bids.sort_by(|a, b| a.round.cmp(&b.round));

        let last_bid = bids.last().unwrap();
        let last_bid_db = clickhouse.get_latest_bid().await;
        if let Some(last_bid_db) = last_bid_db {
            if last_bid_db.round == last_bid.round {
                tracing::debug!(?last_bid_db.round, ?last_bid.round, "No new round found");
                continue;
            }
        }

        let count = clickhouse.write_bids(bids).await?;
        tracing::debug!(?count.rows, "Written bids to clickhouse");
    }
}

#[derive(Debug, Clone)]
pub struct HistoricalBidsService {
    client: S3Client,
}

impl HistoricalBidsService {
    pub const BUCKET_NAME: &str = "timeboost-auctioneer-arb1";
    pub const PREFIX: &str = "uw2/validated-timeboost-bids/";

    pub async fn new() -> eyre::Result<Self> {
        let client = S3Client::new(Self::BUCKET_NAME, Self::PREFIX).await?;
        Ok(Self { client })
    }

    pub async fn request(&self) -> eyre::Result<Vec<BidData>> {
        let latest_file = self.client.get_latest_bid_file().await?;
        let bids = self.client.read_file(&latest_file).await?;
        Ok(bids)
    }
}

#[async_trait::async_trait]
impl Service<()> for HistoricalBidsService {
    type Response = Vec<BidData>;
    type Error = eyre::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: ()) -> Self::Future {
        let this = self.clone();
        Box::pin(async move { this.request().await })
    }
}

/// Client for accessing historical bids from S3 location provided by offchain labs.
#[derive(Debug, Clone)]
pub struct S3Client {
    client: Client,
    bucket_name: String,
    prefix: String,
}

impl S3Client {
    pub async fn new(bucket_name: &str, prefix: &str) -> eyre::Result<Self> {
        // For public S3 buckets, we need to configure the client to not sign requests
        // This is equivalent to AWS CLI's --no-sign-request flag

        let config = aws_config::defaults(BehaviorVersion::latest())
            .region("us-west-2") // Try us-west-2 region for blockchain-related buckets
            .no_credentials() // This is the key - no credentials for unsigned requests
            .load()
            .await;

        // Create S3 client with the shared config
        let client = Client::new(&config);

        Ok(Self {
            client,
            bucket_name: bucket_name.to_string(),
            prefix: prefix.to_string(),
        })
    }

    // gets the latest bid file (use last_modified)
    async fn get_latest_bid_file(&self) -> eyre::Result<S3ObjectInfo> {
        let files = self.get_all_bid_files().await?;

        files
            .into_iter()
            .max_by(|a, b| match (&a.last_modified, &b.last_modified) {
                (Some(a_time), Some(b_time)) => a_time.cmp(b_time),
                (Some(_), None) => std::cmp::Ordering::Greater,
                (None, Some(_)) => std::cmp::Ordering::Less,
                (None, None) => std::cmp::Ordering::Equal,
            })
            .ok_or_else(|| eyre::eyre!("No bid files found"))
    }

    // gets all bid files in the bucket
    async fn get_all_bid_files(&self) -> eyre::Result<Vec<S3ObjectInfo>> {
        let mut objects = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket_name)
                .prefix(&self.prefix);

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let response = request.send().await?;

            let contents = response.contents();
            for object in contents {
                if let Some(key) = object.key() {
                    // Only include files that end with .csv.gzip
                    if key.ends_with(".csv.gzip") {
                        objects.push(S3ObjectInfo {
                            key: key.to_string(),
                            size: object.size().unwrap_or(0),
                            last_modified: object.last_modified().map(|dt| dt.to_string()),
                            etag: object.e_tag().map(|s| s.to_string()),
                        });
                    }
                }
            }

            if response.is_truncated() == Some(true) {
                continuation_token = response.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        Ok(objects)
    }

    // read a file from S3. decompresses the Gz compressed binary
    async fn read_file(&self, file: &S3ObjectInfo) -> eyre::Result<Vec<BidData>> {
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(&file.key)
            .send()
            .await?;

        let body = response.body.collect().await?;
        let compressed_data = body.into_bytes();
        // Decompress the gzipped data
        let mut decoder = GzDecoder::new(compressed_data.as_ref());
        let mut decompressed_data = Vec::new();
        decoder.read_to_end(&mut decompressed_data)?;

        // Parse CSV data
        let csv_str = String::from_utf8(decompressed_data)?;

        let mut reader = csv::Reader::from_reader(csv_str.as_bytes());
        let mut bids = Vec::new();

        for result in reader.deserialize() {
            let bid: BidData = result?;
            bids.push(bid);
        }

        Ok(bids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    pub const BUCKET_NAME: &str = "timeboost-auctioneer-arb1";
    pub const PREFIX: &str = "uw2/validated-timeboost-bids/";

    #[tokio::test]
    async fn test_get_latest_bid_file() {
        let client = S3Client::new(BUCKET_NAME, PREFIX).await.unwrap();
        let _ = client
            .get_latest_bid_file()
            .await
            .expect("Failed to get latest bid file");
    }

    #[tokio::test]
    async fn test_read_file() {
        let client = S3Client::new(BUCKET_NAME, PREFIX)
            .await
            .expect("Failed to create S3 client");

        // Get the latest file to test reading
        let latest_file = client
            .get_latest_bid_file()
            .await
            .expect("Failed to get latest bid file");

        let bids = client
            .read_file(&latest_file)
            .await
            .expect("Failed to read file");

        // Basic validation
        assert!(!bids.is_empty(), "Should have at least one bid");
    }

    #[tokio::test]
    async fn test_get_all_bid_files() {
        let client = S3Client::new(BUCKET_NAME, PREFIX)
            .await
            .expect("Failed to create S3 client");

        let _ = client
            .get_all_bid_files()
            .await
            .expect("Failed to get all bid files");
    }
}
