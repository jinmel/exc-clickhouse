import yaml
import argparse
from collections import namedtuple

wrapped_symbol_to_symbol = {
    'WETH': 'ETH',
    'WBTC': 'BTC',
    'wstETH': 'stETH',
    'cbBTC': 'BTC',
    'weETH': 'eETH',
    'axlUSDC': 'USDC',
    'SolvBTC': 'BTC',
    'tBTC': 'BTC',
    'TIA.n': 'TIA',
}

def unwrap_symbol(symbol):
    return wrapped_symbol_to_symbol.get(symbol.strip(), '')

def parse_args():
    parser = argparse.ArgumentParser(description='Get address symbols from chain config')
    parser.add_argument(
        '--output-tsv',
        type=str,
        required=True,
        help='Output TSV file path'
    )
    parser.add_argument(
        '--chain-config',
        type=str,
        required=True,
        help='Path to chain config YAML file'
    )
    return parser.parse_args()

TokenInfo = namedtuple('TokenInfo', ['symbol', 'address', 'unwrapped_symbol'])

def main(args):

    with open(args.chain_config, 'r') as f:
        chain_config = yaml.safe_load(f)

    address_symbols = []
    tokens = chain_config['tokens']
    native_token = tokens['native']

    address_symbols.append(TokenInfo(
        symbol=native_token['symbol'],
        address=native_token['address'],
        unwrapped_symbol=''
    ))

    wrapped_natives = tokens['wrappedNatives']
    for wrapped_native in wrapped_natives:
        address_symbols.append(TokenInfo(
            symbol=wrapped_native['symbol'],
            address=wrapped_native['address'],
            unwrapped_symbol=unwrap_symbol(native_token['symbol'])
        ))

    for token in tokens['others']:
        address_symbols.append(TokenInfo(
            symbol=token['symbol'],
            address=token['address'],
            unwrapped_symbol=unwrap_symbol(token['symbol'])
        ))

    with open(args.output_tsv, 'w') as f:
        f.write("symbol\taddress\tunwrapped_symbol\n")
        for address_symbol in address_symbols:
            f.write(f"{address_symbol.symbol}\t{address_symbol.address}\t{address_symbol.unwrapped_symbol}\n")


if __name__ == "__main__":
    main(parse_args())
