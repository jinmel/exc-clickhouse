use exc_clickhouse::timeboost::bids::S3Client;

pub const BUCKET_NAME: &str = "timeboost-auctioneer-arb1";
pub const PREFIX: &str = "uw2/validated-timeboost-bids/";

#[tokio::test]
#[ignore]
async fn test_get_latest_bid_file() {
    let client = S3Client::new(BUCKET_NAME, PREFIX).await.unwrap();
    let _ = client
        .get_latest_bid_file()
        .await
        .expect("Failed to get latest bid file");
}

#[tokio::test]
#[ignore]
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
#[ignore]
async fn test_get_all_bid_files() {
    let client = S3Client::new(BUCKET_NAME, PREFIX)
        .await
        .expect("Failed to create S3 client");

    let _ = client
        .get_all_bid_files()
        .await
        .expect("Failed to get all bid files");
}
