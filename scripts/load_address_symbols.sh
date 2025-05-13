#!/usr/bin/env bash
set -euo pipefail

# ── CONFIG ────────────────────────────────────────────────────────────────
# Optionally override via environment:
CLICKHOUSE_HOST=${CLICKHOUSE_HOST:-localhost}
CLICKHOUSE_PORT=${CLICKHOUSE_PORT:-9000}
CLICKHOUSE_USER=${CLICKHOUSE_USER:-default}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-}
CLICKHOUSE_DATABASE=${CLICKHOUSE_DATABASE:-default}

# TSV file to import (default: address_symbols.TSV)
TSV_FILE=${1:-address_symbols.tsv}

if [[ ! -f "$TSV_FILE" ]]; then
  echo "Error: TSV file not found: $TSV_FILE" >&2
  exit 1
fi

# ── IMPORT ────────────────────────────────────────────────────────────────
echo "⟳ Inserting data from '$TSV_FILE' into cex.address_symbols…"

clickhouse client \
  --host     "$CLICKHOUSE_HOST" \
  --port     "$CLICKHOUSE_PORT" \
  --user     "$CLICKHOUSE_USER" \
  ${CLICKHOUSE_PASSWORD:+--password="$CLICKHOUSE_PASSWORD"} \
  --query="INSERT INTO cex.address_symbols FORMAT TSVWithNames" \
  < "$TSV_FILE"

echo "✔ Done."

