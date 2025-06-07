use exc_clickhouse::streams::{
    WebsocketStream, binance::BinanceClient, bybit::BybitClient, coinbase::CoinbaseClient,
    kucoin::KucoinClient, okx::OkxClient,
};
use futures::StreamExt;
use tokio::time::{Duration, timeout};

#[tokio::test]
#[ignore]
async fn test_binance_stream_event() {
    let symbols = vec![
        "btcusdt",
        "ethusdt",
        "bnbusdt",
        "xrpusdt",
        "solusdt",
        "dogeusdt",
        "adausdt",
        "linkusdt",
        "ltcusdt",
        "maticusdt",
    ];
    let client = BinanceClient::builder()
        .add_symbols(symbols)
        .build()
        .unwrap();
    let mut stream = client.stream_events().await.unwrap();
    for _ in 0..50 {
        let result = timeout(Duration::from_secs(10), stream.next()).await;
        assert!(result.is_ok(), "timed out waiting for event");
        let item = result.unwrap();
        assert!(item.is_some(), "no event received");
        assert!(item.unwrap().is_ok(), "event returned error");
    }
}

#[tokio::test]
#[ignore]
async fn test_bybit_stream_event() {
    let symbols = vec![
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "XRPUSDT",
        "DOGEUSDT",
        "TRXUSDT",
        "BNBUSDT",
        "ADAUSDT",
        "LTCUSDT",
        "MATICUSDT",
    ];
    let client = BybitClient::builder().add_symbols(symbols).build().unwrap();
    let mut stream = client.stream_events().await.unwrap();
    for _ in 0..50 {
        let result = timeout(Duration::from_secs(10), stream.next()).await;
        assert!(result.is_ok(), "timed out waiting for event");
        let item = result.unwrap();
        assert!(item.is_some(), "no event received");
        assert!(item.unwrap().is_ok(), "event returned error");
    }
}

#[tokio::test]
#[ignore]
async fn test_coinbase_stream_event() {
    let symbols = vec![
        "BTC-USD",
        "ETH-USD",
        "SOL-USD",
        "ADA-USD",
        "DOGE-USD",
        "AVAX-USD",
        "XRP-USD",
        "LTC-USD",
        "LINK-USD",
        "MATIC-USD",
    ];
    let client = CoinbaseClient::builder()
        .add_symbols(symbols)
        .build()
        .unwrap();
    let mut stream = client.stream_events().await.unwrap();
    for _ in 0..50 {
        let result = timeout(Duration::from_secs(10), stream.next()).await;
        assert!(result.is_ok(), "timed out waiting for event");
        let item = result.unwrap();
        assert!(item.is_some(), "no event received");
        assert!(item.unwrap().is_ok(), "event returned error");
    }
}

#[tokio::test]
#[ignore]
async fn test_okx_stream_event() {
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
    for _ in 0..50 {
        let result = timeout(Duration::from_secs(10), stream.next()).await;
        assert!(result.is_ok(), "timed out waiting for event");
        let item = result.unwrap();
        assert!(item.is_some(), "no event received");
        assert!(item.unwrap().is_ok(), "event returned error");
    }
}

#[tokio::test]
#[ignore]
async fn test_kucoin_stream_event() {
    let client = KucoinClient::builder()
        .add_symbols(vec![
            "BTC-USDT",
            "ETH-USDT",
            "XRP-USDT",
            "BCH-USDT",
            "ADA-USDT",
            "DOGE-USDT",
            "SOL-USDT",
            "DOT-USDT",
            "TRX-USDT",
            "LTC-USDT",
        ])
        .build()
        .await
        .unwrap();
    let mut stream = client.stream_events().await.unwrap();

    for _ in 0..50 {
        let result = timeout(Duration::from_secs(10), stream.next()).await;
        assert!(result.is_ok(), "timed out waiting for event");
        let item = result.unwrap();
        assert!(item.is_some(), "no event received");
        assert!(item.unwrap().is_ok(), "event returned error");
    }
}
