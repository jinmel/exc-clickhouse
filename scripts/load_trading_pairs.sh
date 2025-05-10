#!/usr/bin/env bash
set -euo pipefail

# ── CONFIG ────────────────────────────────────────────────────────────────
CLICKHOUSE_HOST=${CLICKHOUSE_HOST:-localhost}
CLICKHOUSE_PORT=${CLICKHOUSE_PORT:-9000}
CLICKHOUSE_USER=${CLICKHOUSE_USER:-default}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-}
CLICKHOUSE_DATABASE=${CLICKHOUSE_DATABASE:-default}

BINANCE_EXCHANGE_NAME="binance"
BINANCE_API="https://api.binance.com/api/v3/exchangeInfo"

# ── 1. FETCH ──────────────────────────────────────────────────────────────
echo "⟳ Downloading exchangeInfo.json…"
curl -sS "$BINANCE_API" -o exchangeInfo.json

# ── 2. PARSE & EXPLODE ────────────────────────────────────────────────────
# Columns: exchange, trading_type, pair, base_asset, quote_asset
echo "⟳ Generating trading_pairs.tsv…"
jq -r --arg ex "$BINANCE_EXCHANGE_NAME" '
  .symbols[]
  | . as $orig                                      # save the full object
  | $orig.permissionSets[0]                         # take its first permission-set array
    | map(select(test("^TRD_") | not))[]           # filter out TRD_* and explode
  | [ $ex, ., $orig.symbol, $orig.baseAsset, $orig.quoteAsset ]
  | @tsv
' exchangeInfo.json > trading_pairs.tsv

# ── 3. BULK INSERT ─────────────────────────────────────────────────────────
echo "⟳ Inserting $(wc -l < trading_pairs.tsv) rows into cex.trading_pairs…"
clickhouse-client \
  --host     "$CLICKHOUSE_HOST" \
  --port     "$CLICKHOUSE_PORT" \
  --user     "$CLICKHOUSE_USER" \
  ${CLICKHOUSE_PASSWORD:+--password="$CLICKHOUSE_PASSWORD"} \
  --database "$CLICKHOUSE_DATABASE" \
  --query="INSERT INTO cex.trading_pairs (exchange, trading_type, pair, base_asset, quote_asset) FORMAT TabSeparated" \
  < trading_pairs.tsv

rm trading_pairs.tsv exchangeInfo.json

echo "✔ Done."

