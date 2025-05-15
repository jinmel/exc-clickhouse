#!/usr/bin/env bash
set -euo pipefail

# ── CONFIG ───────────────────────────────────────────────────────────────────
CLICKHOUSE_HOST=${CLICKHOUSE_HOST:-localhost}
CLICKHOUSE_PORT=${CLICKHOUSE_PORT:-9000}
CLICKHOUSE_USER=${CLICKHOUSE_USER:-default}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-}

# TSV file to import (default: pools.tsv)
TSV_FILE=${1:-pools.tsv}

if [[ ! -f "$TSV_FILE" ]]; then
  echo "Error: TSV file not found: $TSV_FILE" >&2
  exit 1
fi

echo "⟳ Loading '$TSV_FILE' → ethereum.pools…"

awk -F'\t' -v OFS='\t' '
  NR == 1 {
    # Emit 6-col header: add curve_lp_token + init_block
    print $1, $2, $3, $4, "curve_lp_token", "init_block"
    next
  }
  {
    # Columns 1–4 are already correct;
    # append empty for curve_lp_token and 0 for init_block
    print $1, $2, $3, $4, "\\N", 0
  }
' "$TSV_FILE" \
| clickhouse client \
    --host     "$CLICKHOUSE_HOST" \
    --port     "$CLICKHOUSE_PORT" \
    --user     "$CLICKHOUSE_USER" \
    ${CLICKHOUSE_PASSWORD:+--password="$CLICKHOUSE_PASSWORD"} \
    --query="INSERT INTO ethereum.pools FORMAT TabSeparatedWithNames"

echo "✔ Done."

