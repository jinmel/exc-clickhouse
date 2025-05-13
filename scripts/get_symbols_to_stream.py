import csv
import argparse


def parse_args():
    parser = argparse.ArgumentParser(description='Filter trading pairs containing symbols in address_symbols.tsv')
    parser.add_argument(
        '--trading-pairs',
        type=str,
        required=True,
        help='Path to trading_pairs.tsv file'
    )
    parser.add_argument(
        '--address-symbols',
        type=str,
        required=True,
        help='Path to address_symbols.tsv file'
    )
    parser.add_argument(
        '--output',
        type=str,
        required=True,
        help='Path to output TSV file'
    )
    return parser.parse_args()


def main():

    quote_asset_to_use = ['USDT', 'USDC', 'BTC', 'ETH']
    args = parse_args()

    # Read symbols from address_symbols.tsv
    symbols = set()
    with open(args.address_symbols, 'r') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader)  # Skip header
        for row in reader:
            if row:  # Make sure row is not empty
                symbols.add(row[0])  # Add symbol from column 0

    print(f"Loaded {len(symbols)} symbols from {args.address_symbols}")

    # Filter trading pairs containing those symbols
    filtered_pairs = []
    with open(args.trading_pairs, 'r') as f:
        reader = csv.reader(f, delimiter='\t')

        for row in reader:
            if row:  # Make sure row is not empty
                if row[1] != 'SPOT':
                    continue
                base_asset = row[3]
                quote_asset = row[4]

                for symbol in symbols:
                    if symbol == base_asset and quote_asset in quote_asset_to_use:
                        filtered_pairs.append([row[2]])

    # Write filtered pairs to output file
    with open(args.output, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='\t')
        writer.writerows(filtered_pairs)

    print(f"Filtered {len(filtered_pairs)-1} trading pairs to {args.output}")


if __name__ == "__main__":
    main()

