#!/usr/bin/env python3
"""
Complete trading pairs fetcher script.

Fetches SPOT and FUTURES trading pairs from all supported exchanges and exports to YAML format
compatible with the Rust symbols.yaml structure.
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Set

from adapter_factory import AdapterFactory
from yaml_exporter import YAMLExporter
from models import TradingPair


def load_base_assets(file_path: str) -> Set[str]:
    """
    Load base assets from a newline-separated text file.
    
    Args:
        file_path: Path to the text file containing base assets
        
    Returns:
        Set of base asset symbols (uppercase)
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            base_assets = set()
            for line in f:
                asset = line.strip().upper()
                if asset:  # Skip empty lines
                    base_assets.add(asset)
        return base_assets
    except FileNotFoundError:
        print(f"❌ Base assets file not found: {file_path}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error reading base assets file: {e}")
        sys.exit(1)


def load_quote_assets(file_path: str) -> Set[str]:
    """
    Load quote assets from a newline-separated text file.
    
    Args:
        file_path: Path to the text file containing quote assets
        
    Returns:
        Set of quote asset symbols (uppercase)
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            quote_assets = set()
            for line in f:
                asset = line.strip().upper()
                if asset:  # Skip empty lines
                    quote_assets.add(asset)
        return quote_assets
    except FileNotFoundError:
        print(f"❌ Quote assets file not found: {file_path}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error reading quote assets file: {e}")
        sys.exit(1)


def filter_trading_pairs(
    trading_pairs: List[TradingPair], 
    base_assets: Set[str] = None, 
    quote_assets: Set[str] = None
) -> List[TradingPair]:
    """
    Filter trading pairs by base assets and/or quote assets.
    
    Args:
        trading_pairs: List of all trading pairs
        base_assets: Set of allowed base assets (optional)
        quote_assets: Set of allowed quote assets (optional)
        
    Returns:
        Filtered list of trading pairs
    """
    filtered_pairs = []
    for pair in trading_pairs:
        # Check base asset filter
        base_match = base_assets is None or pair.base_asset.upper() in base_assets
        
        # Check quote asset filter  
        quote_match = quote_assets is None or pair.quote_asset.upper() in quote_assets
        
        # Include pair only if it matches both filters (when provided)
        if base_match and quote_match:
            filtered_pairs.append(pair)
    
    return filtered_pairs


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch SPOT and FUTURES trading pairs from cryptocurrency exchanges",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_fetcher.py                                      # Fetch all trading pairs
  python run_fetcher.py --base_assets assets.txt             # Filter by base assets only
  python run_fetcher.py --quote_assets quotes.txt            # Filter by quote assets only
  python run_fetcher.py --base_assets assets.txt --quote_assets quotes.txt  # Filter by both
  python run_fetcher.py --futures-only                       # Fetch only from Binance Futures adapter
  
Base/Quote assets file format (one asset per line):
  BTC
  ETH
  USDT
  USDC
        """
    )
    
    parser.add_argument(
        '--base_assets',
        type=str,
        help='Path to text file containing base assets to filter by (one per line)'
    )
    
    parser.add_argument(
        '--quote_assets',
        type=str,
        help='Path to text file containing quote assets to filter by (one per line)'
    )

    parser.add_argument(
        '--futures-only',
        action='store_true',
        help='Fetch only from the Binance Futures adapter'
    )
    
    return parser.parse_args()


async def main():
    """Main function to fetch and export trading pairs."""
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    print("🚀 Starting Multi-Exchange SPOT & FUTURES Trading Pairs Fetcher")
    print("=" * 60)
    
    # Load base assets filter if provided
    base_assets_filter = None
    if args.base_assets:
        print(f"📋 Loading base assets filter from: {args.base_assets}")
        base_assets_filter = load_base_assets(args.base_assets)
        print(f"🎯 Filtering by {len(base_assets_filter)} base assets: {sorted(list(base_assets_filter))}")
        print()
    
    # Load quote assets filter if provided
    quote_assets_filter = None
    if args.quote_assets:
        print(f"📋 Loading quote assets filter from: {args.quote_assets}")
        quote_assets_filter = load_quote_assets(args.quote_assets)
        print(f"�� Filtering by {len(quote_assets_filter)} quote assets: {sorted(list(quote_assets_filter))}")
        print()
    
    # Initialize factory
    factory = AdapterFactory()
    if args.futures_only:
        selected_exchanges = ['binance-futures']
    else:
        selected_exchanges = factory.get_available_exchanges()
    
    print(f"📡 Fetching from {len(selected_exchanges)} exchanges:")
    for exchange in selected_exchanges:
        print(f"  • {exchange.title()}")
    print()
    
    # Fetch from all exchanges
    try:
        start_time = datetime.now()
        responses = await factory.fetch_from_all_exchanges(exchanges=selected_exchanges, max_concurrent=3)
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Get statistics
        stats = factory.get_summary_stats(responses)
        
        print("📊 Fetch Results:")
        print(f"  ✅ Successful exchanges: {stats['successful_exchanges']}")
        print(f"  ❌ Failed exchanges: {stats['failed_exchanges']}")
        print(f"  📈 Total pairs fetched: {stats['total_pairs']}")
        print(f"  ⏱️  Total time: {duration:.2f} seconds")
        print()
        
        # Show breakdown by exchange
        print("📋 Exchange Breakdown:")
        for exchange, count in stats['exchange_breakdown'].items():
            print(f"  {exchange.title():>10}: {count:>4} pairs")
        print()
        
        # Show any failures
        if stats['failed_exchanges'] > 0:
            print("❌ Failed Exchanges:")
            for exchange, error in stats['errors'].items():
                print(f"  {exchange}: {error}")
            print()
        
        # Get all trading pairs
        all_pairs = factory.get_all_trading_pairs(responses)
        
        if not all_pairs:
            print("❌ No trading pairs retrieved. Exiting.")
            return 1
        
        # Apply filters if provided
        if base_assets_filter or quote_assets_filter:
            print("🔍 Applying filters...")
            if base_assets_filter and quote_assets_filter:
                print(f"  📋 Base assets: {sorted(list(base_assets_filter))}")
                print(f"  💰 Quote assets: {sorted(list(quote_assets_filter))}")
            elif base_assets_filter:
                print(f"  📋 Base assets only: {sorted(list(base_assets_filter))}")
            else:
                print(f"  💰 Quote assets only: {sorted(list(quote_assets_filter))}")
            
            original_count = len(all_pairs)
            all_pairs = filter_trading_pairs(all_pairs, base_assets_filter, quote_assets_filter)
            filtered_count = len(all_pairs)
            
            print(f"  📉 Filtered from {original_count} to {filtered_count} pairs")
            print(f"  📊 Reduction: {original_count - filtered_count} pairs ({((original_count - filtered_count) / original_count * 100):.1f}%)")
            
            if filtered_count == 0:
                print("❌ No trading pairs match the applied filters. Exiting.")
                return 1
            
            # Show filtered breakdown by exchange
            print("\n📋 Filtered Exchange Breakdown:")
            exchange_counts = {}
            for pair in all_pairs:
                exchange = pair.exchange.title()
                exchange_counts[exchange] = exchange_counts.get(exchange, 0) + 1
            
            for exchange in sorted(exchange_counts.keys()):
                print(f"  {exchange:>10}: {exchange_counts[exchange]:>4} pairs")
            print()
        
        # Export to YAML
        print("📝 Exporting to YAML...")
        exporter = YAMLExporter()
        trading_pairs_list = exporter.create_trading_pairs_list(all_pairs)
        
        # Write to file
        output_filename = "trading_pairs_filtered.yaml" if base_assets_filter or quote_assets_filter else "trading_pairs_flat.yaml"
        output_file = Path(output_filename)
        yaml_content = exporter.to_yaml_string(trading_pairs_list)
        
        with open(output_file, 'w') as f:
            f.write(yaml_content)
        
        # Validate the output
        if exporter.validate_yaml_format(yaml_content):
            print(f"✅ Successfully exported {len(all_pairs)} pairs to {output_file}")
            print(f"📁 File size: {len(yaml_content)} bytes")
        else:
            print("❌ YAML validation failed")
            return 1
        
        print(f"\n🎉 Successfully completed! Output saved to: {output_file.absolute()}")
        return 0
        
    except Exception as e:
        logger.error(f"Failed to fetch trading pairs: {e}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 