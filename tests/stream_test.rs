use exc_clickhouse::streams::{
    WebsocketStream, binance::BinanceClient, bybit::BybitClient, coinbase::CoinbaseClient, 
    hyperliquid::HyperliquidClient,
    kraken::KrakenClient, kucoin::KucoinClient, okx::OkxClient,
};
use futures::StreamExt;
use rustls::crypto::ring::default_provider;
use std::sync::Once;
use tokio::time::{Duration, timeout};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

static INIT: Once = Once::new();

fn init_tracing() {
    INIT.call_once(|| {
        // Install crypto provider for TLS connections
        let _ = default_provider().install_default();
        
        let _ = FmtSubscriber::builder()
            .with_env_filter(EnvFilter::new("exc_clickhouse=trace"))
            .with_test_writer()
            .try_init();
    });
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

    let result = timeout(Duration::from_secs(20), async {
        for _ in 0..100 {
            let item = stream.next().await;
            assert!(item.is_some(), "no event received");
            assert!(item.unwrap().is_ok(), "event returned error");
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "timed out waiting for 100 events within 20 seconds"
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

    let result = timeout(Duration::from_secs(20), async {
        for _ in 0..100 {
            let item = stream.next().await;
            assert!(item.is_some(), "no event received");
            assert!(item.unwrap().is_ok(), "event returned error");
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "timed out waiting for 100 events within 20 seconds"
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

    let result = timeout(Duration::from_secs(20), async {
        for _ in 0..100 {
            let item = stream.next().await;
            assert!(item.is_some(), "no event received");
            assert!(item.unwrap().is_ok(), "event returned error");
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "timed out waiting for 100 events within 20 seconds"
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

    let result = timeout(Duration::from_secs(20), async {
        for _ in 0..100 {
            let item = stream.next().await;
            assert!(item.is_some(), "no event received");
            assert!(item.unwrap().is_ok(), "event returned error");
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "timed out waiting for 100 events within 20 seconds"
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

    let result = timeout(Duration::from_secs(20), async {
        for _ in 0..100 {
            let item = stream.next().await;
            assert!(item.is_some(), "no event received");
            assert!(item.unwrap().is_ok(), "event returned error");
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "timed out waiting for 100 events within 20 seconds"
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

    let result = timeout(Duration::from_secs(20), async {
        for _ in 0..100 {
            let item = stream.next().await;
            assert!(item.is_some(), "no event received");
            assert!(item.unwrap().is_ok(), "event returned error");
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "timed out waiting for 100 events within 20 seconds"
    );
}

#[tokio::test]
#[ignore]
async fn test_hyperliquid_stream_event() {
    init_tracing();
    let symbols = vec![
        "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "LTC", "LINK", "MATIC",
    ];
    let client = HyperliquidClient::builder()
        .add_symbols(symbols)
        .build()
        .unwrap();
    let mut stream = client.stream_events().await.unwrap();

    let result = timeout(Duration::from_secs(20), async {
        for _ in 0..100 {
            let item = stream.next().await;
            assert!(item.is_some(), "no event received");
            assert!(item.unwrap().is_ok(), "event returned error");
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "timed out waiting for 100 events within 20 seconds"
    );
}
