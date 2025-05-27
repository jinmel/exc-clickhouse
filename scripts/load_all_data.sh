#!/usr/bin/env bash
set -euo pipefail

# ─── Usage Parsing ────────────────────────────────────────────────────────────
usage() {
  cat <<EOF
Usage: $0 [--clean | --skip-clean] [--help]

Options:
  --clean        Truncate tables before loading (default)
  --skip-clean   Do not truncate tables, just load data
  -h, --help     Show this help message and exit
EOF
  exit 1
}

# Default behavior: clean tables
CLEAN=true

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --clean)
      CLEAN=true
      shift
      ;;
    --skip-clean)
      CLEAN=false
      shift
      ;;
    -h|--help)
      usage
      ;;
    *)
      echo "Unknown option: $1"
      usage
      ;;
  esac
done

# ─── ClickHouse Connection Defaults ──────────────────────────────────────────
: "${CLICKHOUSE_HOST:=localhost}"
: "${CLICKHOUSE_PORT:=9000}"
: "${CLICKHOUSE_USER:=default}"
: "${CLICKHOUSE_PASSWORD:=}"

# ─── Tables to clear ──────────────────────────────────────────────────────────
TABLES=(
  "ethereum.pools"
  "cex.address_symbols"
  "cex.trading_pairs"
)

# ─── Optionally Delete All Data ───────────────────────────────────────────────
if [[ "$CLEAN" == true ]]; then
  echo "⟳ Truncating tables before load..."
  for table in "${TABLES[@]}"; do
    echo "  - Truncating $table"
    clickhouse client \
      --host "$CLICKHOUSE_HOST" \
      --port "$CLICKHOUSE_PORT" \
      --user "$CLICKHOUSE_USER" \
      ${CLICKHOUSE_PASSWORD:+--password="$CLICKHOUSE_PASSWORD"} \
      --query="TRUNCATE TABLE IF EXISTS $table"
  done
  echo "✅ Truncation complete."
else
  echo "⟳ Skipping truncation (--skip-clean)."
fi

# ─── Load Data Steps ─────────────────────────────────────────────────────────
echo "⟳ Loading all data into ClickHouse..."

echo "⟳ Loading pools…"
./load_pools.sh

echo "⟳ Loading trading pairs…"
./load_trading_pairs.sh

echo "⟳ Loading address symbols…"
./load_address_symbols.sh
