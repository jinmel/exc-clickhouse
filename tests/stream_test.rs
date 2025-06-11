use exc_clickhouse::streams::{
    WebsocketStream, binance::BinanceClient, bybit::BybitClient, coinbase::CoinbaseClient,
    kraken::KrakenClient, kucoin::KucoinClient, mexc::MexcClient, okx::OkxClient,
};
use futures::StreamExt;
use tokio::time::{Duration, timeout};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

fn init_tracing() {
    let _ = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::new("exc_clickhouse=trace"))
        .with_test_writer()
        .try_init();
}

#[tokio::test]
#[ignore]
async fn test_binance_stream_event() {
    init_tracing();
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

    let result = timeout(Duration::from_secs(10), async {
        for _ in 0..30 {
            let item = stream.next().await;
            assert!(item.is_some(), "no event received");
            assert!(item.unwrap().is_ok(), "event returned error");
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "timed out waiting for 30 events within 10 seconds"
    );
}

#[tokio::test]
#[ignore]
async fn test_bybit_stream_event() {
    init_tracing();
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

    let result = timeout(Duration::from_secs(10), async {
        for _ in 0..30 {
            let item = stream.next().await;
            assert!(item.is_some(), "no event received");
            assert!(item.unwrap().is_ok(), "event returned error");
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "timed out waiting for 30 events within 10 seconds"
    );
}

#[tokio::test]
#[ignore]
async fn test_coinbase_stream_event() {
    init_tracing();
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

    let result = timeout(Duration::from_secs(10), async {
        for _ in 0..30 {
            let item = stream.next().await;
            assert!(item.is_some(), "no event received");
            assert!(item.unwrap().is_ok(), "event returned error");
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "timed out waiting for 30 events within 10 seconds"
    );
}

#[tokio::test]
#[ignore]
async fn test_okx_stream_event() {
    init_tracing();
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

    let result = timeout(Duration::from_secs(10), async {
        for _ in 0..30 {
            let item = stream.next().await;
            assert!(item.is_some(), "no event received");
            assert!(item.unwrap().is_ok(), "event returned error");
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "timed out waiting for 30 events within 10 seconds"
    );
}

#[tokio::test]
#[ignore]
async fn test_kraken_stream_event() {
    init_tracing();
    let symbols = vec![
        "BTC/USD", "ETH/USD", "XRP/USD", "ADA/USD", "DOGE/USD", "SOL/USD", "LTC/USD", "LINK/USD",
        "UNI/USD",
    ];
    let client = KrakenClient::builder()
        .add_symbols(symbols)
        .build()
        .unwrap();
    let mut stream = client.stream_events().await.unwrap();

    let result = timeout(Duration::from_secs(10), async {
        // test 10 messages for kraken since it rarely give us updates
        for _ in 0..10 {
            let item = stream.next().await;
            assert!(item.is_some(), "no event received");
            assert!(item.unwrap().is_ok(), "event returned error");
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "timed out waiting for 30 events within 10 seconds"
    );
}

#[tokio::test]
#[ignore]
async fn test_kucoin_stream_event() {
    init_tracing();
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

    let result = timeout(Duration::from_secs(10), async {
        for _ in 0..30 {
            let item = stream.next().await;
            assert!(item.is_some(), "no event received");
            assert!(item.unwrap().is_ok(), "event returned error");
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "timed out waiting for 30 events within 10 seconds"
    );
}

#[tokio::test]
#[ignore]
async fn test_mexc_stream_event() {
    init_tracing();
    let symbols = vec![
        "BTCUSDT",
        "ETHUSDT",
        "XRPUSDT",
        "ADAUSDT",
        "DOGEUSDT",
        "SOLUSDT",
        "LTCUSDT",
        "LINKUSDT",
        "MATICUSDT",
        "DOTUSDT",
    ];
    let client = MexcClient::builder()
        .add_symbols(symbols)
        .build()
        .unwrap();
    let mut stream = client.stream_events().await.unwrap();

    let result = timeout(Duration::from_secs(10), async {
        for _ in 0..30 {
            let item = stream.next().await;
            assert!(item.is_some(), "no event received");
            assert!(item.unwrap().is_ok(), "event returned error");
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "timed out waiting for 30 events within 10 seconds"
    );
}
