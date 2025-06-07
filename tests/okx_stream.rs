use exc_clickhouse::streams::okx::OkxClient;
use exc_clickhouse::streams::WebsocketStream;
use futures::StreamExt;
use tokio::time::{Duration, timeout};

#[tokio::test]
#[ignore]
async fn test_okx_stream_many_symbols() {
    let symbols = vec![
        "BTC-USDT",
        "ETH-USDT",
        "SOL-USDT",
        "XRP-USDT",
        "DOGE-USDT",
        "TRX-USDT",
        "BNB-USDT",
        "ADA-USDT",
        "LTC-USDT",
        "MATIC-USDT",
    ];
    let client = OkxClient::builder().add_symbols(symbols).build().unwrap();
    let mut stream = client.stream_events().await.unwrap();
    let result = timeout(Duration::from_secs(10), stream.next()).await;
    assert!(result.is_ok(), "timed out waiting for event");
    let item = result.unwrap();
    assert!(item.is_some(), "no event received");
    assert!(item.unwrap().is_ok(), "event returned error");
}
