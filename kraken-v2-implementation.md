# Kraken WebSocket API v2 Implementation

This document outlines the implementation of Kraken WebSocket API v2 support based on the [official documentation](https://docs.kraken.com/api/docs/websocket-v2).

## Overview

The implementation follows the same pattern as other exchanges (Binance, KuCoin) and includes:

1. **Client Module** (`src/streams/kraken/mod.rs`) - Main client with builder pattern
2. **Model Module** (`src/streams/kraken/model.rs`) - Data structures with serde tag matching
3. **Parser Module** (`src/streams/kraken/parser.rs`) - Message parser implementation  
4. **Subscription Module** - Updated `src/streams/subscription.rs` with KrakenSubscription

## Key Features

### Tagged Enum Parsing
Uses serde's `#[serde(tag = "channel")]` for efficient message routing:

```rust
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "channel")]
pub enum Response {
    #[serde(rename = "trade")]
    Trade(TradeMessage),
    #[serde(rename = "ticker")]
    Ticker(TickerMessage),
    #[serde(rename = "status")]
    Status(StatusMessage),
    #[serde(rename = "heartbeat")]
    Heartbeat(HeartbeatMessage),
    #[serde(other)]
    Unknown,
}
```

### TryFrom Implementations
Converts Kraken-specific data to normalized types:

```rust
impl TryFrom<TradeData> for NormalizedTrade {
    type Error = ExchangeStreamError;
    
    fn try_from(trade: TradeData) -> Result<NormalizedTrade, Self::Error> {
        // Parse RFC3339 timestamp to microseconds
        let timestamp = chrono::DateTime::parse_from_rfc3339(&trade.timestamp)?
            .timestamp_micros() as u64;
            
        let side = match trade.side.as_str() {
            "buy" => TradeSide::Buy,
            "sell" => TradeSide::Sell,
            _ => return Err(ExchangeStreamError::MessageError(...)),
        };
        
        Ok(NormalizedTrade::new(
            ExchangeName::Kraken,
            &trade.symbol,
            timestamp,
            side,
            trade.price,
            trade.qty,
        ))
    }
}
```

## API Endpoints

### WebSocket URL
- **Production**: `wss://ws.kraken.com/v2`

### Supported Channels

#### 1. Ticker (Level 1) - Quotes
- **Channel**: `ticker`
- **Purpose**: Best bid/offer and recent trade data
- **Subscription**:
```json
{
    "method": "subscribe",
    "params": {
        "channel": "ticker",
        "symbol": ["BTC/USD", "ETH/USD"],
        "snapshot": true
    },
    "req_id": 123456
}
```

#### 2. Trades 
- **Channel**: `trade`  
- **Purpose**: Real-time trade events
- **Subscription**:
```json
{
    "method": "subscribe", 
    "params": {
        "channel": "trade",
        "symbol": ["BTC/USD", "ETH/USD"],
        "snapshot": true
    },
    "req_id": 123456
}
```

## Message Formats

### Trade Message
```json
{
    "channel": "trade",
    "type": "update",
    "data": [
        {
            "symbol": "BTC/USD",
            "side": "buy",
            "price": 50000.0,
            "qty": 0.1,
            "ord_type": "market",
            "trade_id": 123456,
            "timestamp": "2023-09-25T07:49:37.708706Z"
        }
    ]
}
```

### Ticker Message  
```json
{
    "channel": "ticker",
    "type": "update",
    "data": [
        {
            "symbol": "BTC/USD",
            "bid": 49950.0,
            "bid_qty": 1.5,
            "ask": 50050.0,
            "ask_qty": 2.0,
            "last": 50000.0,
            "volume": 123.45,
            "vwap": 49975.0,
            "low": 49500.0,
            "high": 50500.0,
            "change": 500.0,
            "change_pct": 1.01
        }
    ]
}
```

## Usage Example

```rust
use exc_clickhouse::streams::kraken::KrakenClient;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let client = KrakenClient::builder()
        .add_symbols(vec!["BTC/USD", "ETH/USD", "ALGO/USD"])
        .build()?;
        
    let mut stream = client.stream_events().await?;
    
    while let Some(event) = stream.next().await {
        match event? {
            NormalizedEvent::Trade(trade) => {
                println!("Trade: {} {} {} @ {}", 
                    trade.symbol, trade.side, trade.amount, trade.price);
            }
            NormalizedEvent::Quote(quote) => {
                println!("Quote: {} bid: {} @ {} ask: {} @ {}", 
                    quote.symbol, quote.bid_amount, quote.bid_price,
                    quote.ask_amount, quote.ask_price);
            }
        }
    }
    
    Ok(())
}
```

## Error Handling

The implementation handles various error scenarios:

- **Connection Errors**: WebSocket connection issues
- **Subscription Errors**: Failed subscriptions with error messages
- **Parse Errors**: Invalid JSON or unexpected message formats
- **Data Errors**: Invalid timestamps, prices, or trade sides

## Heartbeat/Ping

- **Interval**: 50 seconds
- **Message**: `{"method": "ping", "req_id": 123456}`
- **Purpose**: Keep connection alive and detect disconnections

## Key Differences from v1

1. **Message Format**: JSON instead of array-based format
2. **Subscription**: Uses `method: "subscribe"` with structured params
3. **Channels**: Named channels (`ticker`, `trade`) instead of numbered streams  
4. **Timestamps**: RFC3339 format instead of Unix timestamps
5. **Heartbeat**: Structured ping/pong messages

## Files Created/Modified

### New Files:
- `src/streams/kraken/mod.rs` - Main client implementation
- `src/streams/kraken/model.rs` - Data structures and TryFrom traits
- `src/streams/kraken/parser.rs` - Message parser

### Modified Files:
- `src/streams/mod.rs` - Added kraken module
- `src/streams/subscription.rs` - Added KrakenSubscription implementation

## Dependencies

The implementation uses existing dependencies:
- `serde` - JSON serialization/deserialization with tagging
- `chrono` - RFC3339 timestamp parsing  
- `tokio-tungstenite` - WebSocket client
- `async-trait` - Async trait support

## Testing

To test the implementation, you can use the existing test patterns from other exchanges or create integration tests that:

1. Connect to Kraken WebSocket v2
2. Subscribe to ticker and trade channels
3. Verify message parsing and normalization
4. Test error handling scenarios

The implementation is ready for use once the OpenSSL compilation dependencies are resolved in the build environment.