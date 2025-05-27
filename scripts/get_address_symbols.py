import argparse
import csv
import dataclasses
import collections
from datetime import datetime
from typing import Optional, List, Dict
import re

@dataclasses.dataclass
class PoolRecord:
    project: str
    protocol: str
    liquidity_pool_address: str
    token0_address: str
    token0_name: str
    token0_symbol: str
    token0_decimals: int
    token1_address: str
    token1_name: str
    token1_symbol: str
    token1_decimals: int
    fee: int
    tick_spacing: Optional[int]
    factory_address: Optional[str]
    transaction_hash: str
    block_timestamp: datetime
    block_number: int
    block_hash: str
    liquidity_pool_factory_address: str

    @staticmethod
    def _parse_int(value: str) -> Optional[int]:
        return None if value.lower() == 'null' else int(value)

    @staticmethod
    def _parse_datetime(value: str) -> datetime:
        # Adjust format string if your timestamps differ
        return datetime.fromisoformat(value)

    @classmethod
    def from_dict(cls, row: dict) -> 'PoolRecord':
        return cls(
            project=row['project'],
            protocol=row['protocol'],
            liquidity_pool_address=row['liquidity_pool_address'],
            token0_address=row['token0_address'],
            token0_name=row['token0_name'],
            token0_symbol=row['token0_symbol'],
            token0_decimals=0 if row['token0_decimals'] == 'null' else int(row['token0_decimals']),
            token1_address=row['token1_address'],
            token1_name=row['token1_name'],
            token1_symbol=row['token1_symbol'],
            token1_decimals=0 if row['token1_decimals'] == 'null' else int(row['token1_decimals']),
            fee=0 if row['fee'] == 'null' else int(row['fee']),
            tick_spacing=cls._parse_int(row['tick_spacing']),
            factory_address=None if row['factory_address'].lower() == 'null' else row['factory_address'],
            transaction_hash=row['transaction_hash'],
            block_timestamp=cls._parse_datetime(row['block_timestamp']),
            block_number=int(row['block_number']),
            block_hash=row['block_hash'],
            liquidity_pool_factory_address=row['liquidity_pool_factory_address'],
        )

    def validate(self) -> bool:

        if self.token0_address == 'null' or self.token1_address == 'null':
            # print(f"Invalid token addresses: {self.token0_address}, {self.token1_address}")
            return False

        if self.token0_symbol == 'null' or self.token1_symbol == 'null':
            # print(f"Invalid token symbols: {self.token0_symbol}, {self.token1_symbol}")
            return False

        if self.token0_address == 'null' and self.token1_address == 'null':
            # print(f"Both token addresses are null: {self.token0_address}, {self.token1_address}")
            return False

        if check_address(self.token0_address) is False:
            print(f"Invalid token0 address: {self.token0_address}")
            return False

        if check_address(self.token1_address) is False:
            print(f"Invalid token1 address: {self.token1_address}")
            return False

        return True


def nul_stripper(f):
    for line in f:
        yield line.replace('\0', '').replace('\r', '')


def check_address(addr):
    if len(addr) != 42:
        raise ValueError(f"Invalid address length: {addr}")
    clean = addr[2:] if addr.lower().startswith("0x") else addr
    try:
        bytes.fromhex(clean)          # or: binascii.unhexlify(clean)
    except ValueError:
        return False
    else:
        return True


_ASCII_RE = re.compile(r"^[A-za-z0-9\.\-\,\$ ]+$")


def is_ascii(s: str) -> bool:
    try:
        s.encode('ascii')
    except UnicodeEncodeError:
        return False
    return bool(_ASCII_RE.fullmatch(s))


def read_pool_records(csv_path: str) -> List[PoolRecord]:
    records: List[PoolRecord] = []
    with open(csv_path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(nul_stripper(f))
        for row in reader:
            record = PoolRecord.from_dict(row)
            if record.validate():
                records.append(record)
    return records


@dataclasses.dataclass
class TokenInfo:
    address: str
    name: str
    symbol: str
    unwrapped_symbol: Optional[str]


@dataclasses.dataclass
class PoolInfo:
    protocol: str
    protocol_subtype: str
    address: str
    tokens: List[str]


def parse_args():
    parser = argparse.ArgumentParser(description='Get address symbols from chain config')
    parser.add_argument(
        '--address_symbols_tsv',
        type=str,
        required=True,
        help='Output address_symbols table TSV file path'
    )
    parser.add_argument(
        '--pools_tsv',
        type=str,
        required=True,
        help='Output pools table TSV file path'
    )
    parser.add_argument(
        '--input_csv',
        type=str,
        required=True,
        help='Input CSV file of historical pool logs'
    )
    return parser.parse_args()


_strip_w_leading = re.compile(r'^([^A-Za-z]*)([wW])')


def get_token_info(records: List[PoolRecord], min_count=10) -> Dict[str, TokenInfo]:
    # address to token_info mapping
    token_infos: Dict[str, TokenInfo] = {}
    addr_to_count = collections.defaultdict(int)

    for record in records:
        for addr, name, sym in [
            (record.token0_address, record.token0_name, record.token0_symbol),
            (record.token1_address, record.token1_name, record.token1_symbol),
        ]:
            if addr == 'null' or name == 'null' or sym == 'null':
                continue

            # Skip names with non-ascii characters
            if not (is_ascii(name) and is_ascii(sym)):
                continue

            addr_to_count[addr] += 1

            is_wrapped = name.lower().startswith('wrapped')
            unwrapped_symbol = None
            if is_wrapped:
                unwrapped_symbol= _strip_w_leading.sub(r'\1', sym)

            if addr in token_infos.keys():
                continue

            token_infos[addr] = TokenInfo(
                address=addr,
                name=name,
                symbol=sym,
                unwrapped_symbol=unwrapped_symbol
            )

    filtered = {}
    for addr, info in token_infos.items():
        # Check if the token appears more than once
        if addr_to_count[addr] > min_count:
            filtered[addr] = info
    return filtered


def get_pools(records: List[PoolRecord], token_infos: List[TokenInfo]) -> List[PoolRecord]:
    token_addresses = set()

    for info in token_infos:
        token_addresses.add(info.address)

    pools: List[PoolRecord] = []
    for record in records:
        if record.token0_address not in token_addresses or record.token1_address not in token_addresses:
            continue

        protocol = record.protocol
        parts = protocol.split('_')

        if len(parts) == 1:
            protocol = parts[0]
            protocol_subtype = ''
        else:
            protocol = parts[0]
            protocol_subtype = parts[1]

        if protocol == 'uniswap':
            protocol = 'Uniswap'
        elif protocol == 'sushiswap':
            protocol = 'SushiSwap'
        elif protocol == 'balancer':
            protocol = 'Balancer'
        elif protocol == 'curve':
            protocol = 'Curve'
        elif protocol == 'camelot':
            protocol = 'Camelot'
        elif protocol == 'pancakeswap':
            protocol = 'PancakeSwap'

        protocol_subtype = protocol_subtype.upper()
        pool = PoolInfo(
            protocol=protocol,
            protocol_subtype=protocol_subtype,
            address=record.liquidity_pool_address,
            tokens=[record.token0_address, record.token1_address]
        )
        pools.append(pool)
    return pools


def main(args):
    records = read_pool_records(args.input_csv)
    token_infos = get_token_info(records, min_count=100)
    with open(args.address_symbols_tsv, 'w') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerow(['address', 'symbol', 'unwrapped_symbol'])
        for token_info in token_infos.values():
            writer.writerow([token_info.address, token_info.symbol, token_info.unwrapped_symbol])

    print(f"Token info {len(token_infos.values())} written to {args.address_symbols_tsv}")

    pools = get_pools(records, token_infos.values())
    with open(args.pools_tsv, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerow(['protocol', 'protocol_subtype', 'address', 'tokens'])
        for pool in pools:
            writer.writerow([pool.protocol, pool.protocol_subtype, pool.address, pool.tokens])

    print(f"Pool info {len(pools)} written to {args.pools_tsv}")


if __name__ == "__main__":
    main(parse_args())
