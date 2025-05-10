#!/usr/bin/env bash
set -euo pipefail

# ── CONFIG ────────────────────────────────────────────────────────────────
# Optionally override via environment:
CLICKHOUSE_HOST=${CLICKHOUSE_HOST:-localhost}
CLICKHOUSE_PORT=${CLICKHOUSE_PORT:-9000}
CLICKHOUSE_USER=${CLICKHOUSE_USER:-default}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-}
CLICKHOUSE_DATABASE=${CLICKHOUSE_DATABASE:-default}

# CSV file to import (default: address_symbols.csv)
CSV_FILE=${1:-address_symbols.csv}

if [[ ! -f "$CSV_FILE" ]]; then
  echo "Error: CSV file not found: $CSV_FILE" >&2
  exit 1
fi

# ── IMPORT ────────────────────────────────────────────────────────────────
echo "⟳ Inserting data from '$CSV_FILE' into cex.address_symbols…"

clickhouse-client \
  --host     "$CLICKHOUSE_HOST" \
  --port     "$CLICKHOUSE_PORT" \
  --user     "$CLICKHOUSE_USER" \
  ${CLICKHOUSE_PASSWORD:+--password="$CLICKHOUSE_PASSWORD"} \
  --database "$CLICKHOUSE_DATABASE" \
  --query="INSERT INTO cex.address_symbols FORMAT CSVWithNames" \
  < "$CSV_FILE"

echo "✔ Done."

