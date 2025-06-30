use exc_clickhouse::streams::{
    WebsocketStream, binance::BinanceClient, bybit::BybitClient, coinbase::CoinbaseClient, 
    hyperliquid::HyperliquidClient,
    kraken::KrakenClient, kucoin::KucoinClient, okx::OkxClient,
};
use futures::StreamExt;
use rustls::crypto::ring::default_provider;
use std::sync::Once;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;
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
    let cancellation_token = CancellationToken::new();
    let mut stream = client.stream_events(cancellation_token).await.unwrap();

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
    let cancellation_token = CancellationToken::new();
    let mut stream = client.stream_events(cancellation_token).await.unwrap();

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
    let cancellation_token = CancellationToken::new();
    let mut stream = client.stream_events(cancellation_token).await.unwrap();

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
    let cancellation_token = CancellationToken::new();
    let mut stream = client.stream_events(cancellation_token).await.unwrap();

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
    let cancellation_token = CancellationToken::new();
    let mut stream = client.stream_events(cancellation_token).await.unwrap();

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
    let cancellation_token = CancellationToken::new();
    let mut stream = client.stream_events(cancellation_token).await.unwrap();

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
    let cancellation_token = CancellationToken::new();
    let mut stream = client.stream_events(cancellation_token).await.unwrap();

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
async fn test_graceful_shutdown() {
    init_tracing();
    use exc_clickhouse::streams::exchange_stream::ExchangeStreamBuilder;
    use exc_clickhouse::streams::{Parser, Subscription};
    use exc_clickhouse::models::NormalizedEvent;
    use futures::StreamExt;
    use tokio_util::sync::CancellationToken;

    
    // Simple mock parser that doesn't parse anything
    #[derive(Clone, Debug)]
    struct MockParser;
    
    impl Parser<Vec<NormalizedEvent>> for MockParser {
        type Error = exc_clickhouse::streams::ExchangeStreamError;
        
        fn parse(&self, _data: &str) -> Result<Option<Vec<NormalizedEvent>>, Self::Error> {
            // Return None so the stream continues without yielding events
            Ok(None)
        }
    }
    
    // Simple mock subscription
    #[derive(Clone)]
    struct MockSubscription;
    
    impl Subscription for MockSubscription {
        fn to_json(&self) -> Result<Vec<serde_json::Value>, serde_json::Error> {
            Ok(vec![serde_json::json!({"method":"SUBSCRIBE","params":["btcusdt@trade"],"id":1})])
        }
        
        fn heartbeat(&self) -> Option<tokio_tungstenite::tungstenite::Message> {
            None
        }
        
        fn heartbeat_interval(&self) -> Option<std::time::Duration> {
            Some(std::time::Duration::from_secs(30))
        }
    }

    // Create a cancellation token
    let cancellation_token = CancellationToken::new();
    let token_clone = cancellation_token.clone();

    // Create a simple stream builder with an invalid URL to prevent actual connection
    let parser = MockParser;
    let subscription = MockSubscription;
    
    let stream_builder = ExchangeStreamBuilder::new(
        "ws://localhost:99999/invalid",  // Invalid URL to prevent connection
        Some(std::time::Duration::from_secs(30)),
        parser,
        subscription,
    )
    .with_cancellation(cancellation_token);

    let stream = stream_builder.build();

    // Pin the stream for usage
    futures::pin_mut!(stream);

    // Start a task to cancel immediately
    let cancel_task = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        tracing::info!("Triggering cancellation");
        token_clone.cancel();
    });

    // Measure shutdown time
    let start_time = std::time::Instant::now();
    
    // Try to get events from the stream, but it should shut down gracefully
    let mut event_count = 0;
    while let Some(result) = stream.next().await {
        match result {
            Ok(_) => {
                event_count += 1;
                tracing::info!("Received event #{}", event_count);
            }
            Err(e) => {
                tracing::error!("Stream error: {:?}", e);
                break;
            }
        }
    }
    
    let shutdown_duration = start_time.elapsed();
    
    // Wait for the cancel task to complete
    let _ = cancel_task.await;
    
    tracing::info!(
        events_received = event_count,
        shutdown_duration_ms = shutdown_duration.as_millis(),
        "Stream shut down gracefully"
    );
    
    // The test passes if it completes without hanging indefinitely
    tracing::info!("Graceful shutdown test completed successfully");
}
