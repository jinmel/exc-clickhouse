# exc-clickhouse

<div align="center">
  <img src="logo.png" alt="exc-clickhouse logo" width="300" height="300"/>
</div>

A high-performance, real-time cryptocurrency off-chain data collector that streams market data from multiple sources and stores it in ClickHouse for analytics and research.

## Overview

`exc-clickhouse` is a Rust-based application that collects real-time trading data (trades and quotes) from major cryptocurrency exchanges and stores them in a ClickHouse database. It also supports collecting Ethereum blockchain metadata and Arbitrum Timeboost auction data.

### Supported Exchanges

- **Binance**
- **Bybit**
- **OKX**
- **Coinbase**
- **Kraken**
- **KuCoin**

### Data Types Collected

- **Trades**: Real-time executed trades with price, volume, and side information
- **Quotes**: Best bid/ask prices and volumes (order book top-of-book)
- **Ethereum Blocks**: Block metadata including timestamps, gas usage, and transaction counts
- **Timeboost Bids**: Arbitrum express lane auction bid data

## Features

- 🚀 **High Performance**: Built with Rust and Tokio for maximum throughput
- 📊 **Multi-Exchange Support**: Simultaneous data collection from 6+ exchanges
- 🔄 **Real-time Streaming**: WebSocket-based live data feeds
- 💾 **ClickHouse Integration**: Optimized for analytical workloads
- 🛡️ **Fault Tolerance**: Automatic reconnection and error handling
- ⚡ **Rate Limiting**: Built-in rate limiting to respect exchange limits
- 🔧 **Configurable**: YAML-based symbol configuration
- 📈 **Monitoring**: Comprehensive logging and tracing
- 🔄 **Auto-restart**: Configurable task supervision and restart policies

## Architecture

The application uses a multi-task architecture where each exchange runs in its own async task, streaming data through channels to a centralized ClickHouse writer. The system is built using:

- **Tower**: Service abstraction and middleware (rate limiting, timeouts)
- **Tokio**: Async runtime and concurrency
- **ClickHouse**: High-performance columnar database
- **WebSocket Streams**: Real-time data ingestion
- **MPSC Channels**: Inter-task communication

## Installation

### Prerequisites

- Rust 1.70+ (2024 edition)
- ClickHouse database instance
- Network access to exchange WebSocket APIs

### Build from Source

```bash
git clone https://github.com/your-org/exc-clickhouse.git
cd exc-clickhouse
cargo build --release
```

## Configuration

### Environment Variables

Create a `.env` file based on `sample.env`:

```bash
# ClickHouse Configuration
CLICKHOUSE_URL=http://localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASS=your_password
CLICKHOUSE_DATABASE=crypto

# Ethereum RPC (optional)
RPC_URL=https://eth-mainnet.alchemyapi.io/v2/your-key
```

### Symbol Configuration

Configure trading pairs in `symbols.yaml`:

```yaml
- exchange: "Binance"
  market: "SPOT"
  symbols:
    - BTCUSDT
    - ETHUSDT
    - SOLUSDT

- exchange: "Coinbase"
  market: "SPOT"
  symbols:
    - BTC-USD
    - ETH-USD
    - SOL-USD
```

## Usage

### Stream Real-time Data

```bash
# Stream data from all configured exchanges
./target/release/exc-clickhouse stream

# Custom symbols file
./target/release/exc-clickhouse stream --symbols-file custom-symbols.yaml

# Skip certain data sources
./target/release/exc-clickhouse stream --skip-ethereum --skip-timeboost

# Enable auto-restart with exponential backoff
./target/release/exc-clickhouse stream --enable-restart --max-restart-attempts 5
```

### Database Operations

```bash
# Fetch top Binance trading pairs
./target/release/exc-clickhouse db fetch-binance-symbols --output binance-top.yaml --limit 100

# Backfill Timeboost auction data
./target/release/exc-clickhouse db timeboost
```

### Command Line Options

#### Stream Command

- `--symbols-file`: Path to symbols configuration (default: `symbols.yaml`)
- `--batch-size`: Batch size for ClickHouse inserts (default: 500)
- `--skip-ethereum`: Skip Ethereum block metadata collection
- `--skip-clickhouse`: Skip ClickHouse writer (useful for testing)
- `--skip-timeboost`: Skip Timeboost bid collection
- `--rpc-url`: Ethereum RPC URL (overrides env var)
- `--enable-restart`: Enable automatic task restart on failure
- `--max-restart-attempts`: Maximum restart attempts (0 = unlimited)
- `--restart-delay-seconds`: Initial restart delay
- `--max-restart-delay-seconds`: Maximum restart delay for exponential backoff
- `--clickhouse-rate-limit`: Rate limit for ClickHouse requests/second

## Database Schema

### Trades Table (`cex.normalized_trades`)

```sql
CREATE TABLE cex.normalized_trades (
    exchange String,
    symbol String,
    timestamp UInt64,
    side String,
    price Float64,
    amount Float64
) ENGINE = MergeTree()
ORDER BY (exchange, symbol, timestamp);
```

### Quotes Table (`cex.normalized_quotes`)

```sql
CREATE TABLE cex.normalized_quotes (
    exchange String,
    symbol String,
    timestamp UInt64,
    ask_amount Float64,
    ask_price Float64,
    bid_price Float64,
    bid_amount Float64
) ENGINE = MergeTree()
ORDER BY (exchange, symbol, timestamp);
```

### Ethereum Blocks (`ethereum.blocks`)

```sql
CREATE TABLE ethereum.blocks (
    number UInt64,
    timestamp UInt64,
    hash String,
    parent_hash String,
    gas_used UInt64,
    gas_limit UInt64,
    transaction_count UInt32
) ENGINE = MergeTree()
ORDER BY number;
```

### Timeboost Bids (`timeboost.bids`)

```sql
CREATE TABLE timeboost.bids (
    timestamp DateTime64(3),
    chain_id UInt64,
    bidder String,
    express_lane_controller String,
    auction_contract_address String,
    round UInt64,
    amount String,
    signature String
) ENGINE = MergeTree()
PARTITION BY chain_id
PRIMARY KEY round
ORDER BY round;
```



## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built with [Tokio](https://tokio.rs/) for async runtime
- Uses [Tower](https://github.com/tower-rs/tower) for service abstraction
- [ClickHouse](https://clickhouse.com/) for high-performance analytics
- Exchange APIs for providing real-time market data 