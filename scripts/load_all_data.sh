#!/usr/bin/env bash
set -euo pipefail


echo "⟳ Loading all data into ClickHouse…"

echo "⟳ Loading pools…"
./load_pools.sh

echo "⟳ Loading tokens…"
./load_trading_pairs.sh

echo "⟳ Loading trading pairs…"
./load_address_symbols.sh
