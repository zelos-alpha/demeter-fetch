from datetime import date
from enum import Enum
from dataclasses import dataclass
from typing import List


class DataSource(str,Enum):
    big_query = "big_query"
    rpc = "rpc"
    file = "file"


class ChainType(str,Enum):
    ethereum = "ethereum"
    polygon = "polygon"
    optimism = "optimism"
    arbitrum = "arbitrum"
    celo = "celo"


ChainTypeConfig = {
    ChainType.ethereum: {"allow": [DataSource.big_query, DataSource.rpc, DataSource.file]},
    ChainType.polygon: {"allow": [DataSource.big_query, DataSource.rpc, DataSource.file]},
    ChainType.optimism: {"allow": [DataSource.rpc, DataSource.file]},
    ChainType.arbitrum: {"allow": [DataSource.rpc, DataSource.file]},
    ChainType.celo: {"allow": [DataSource.rpc, DataSource.file]},
}


class ToType(str,Enum):
    minute = "minute"
    tick = "tick"


@dataclass
class BigQueryConfig:
    start: date
    end: date
    auth_file: str
    http_proxy: str | None = None


@dataclass
class RpcConfig:
    end_point: str
    start_height: int
    end_height: int
    batch_size: int = 500
    auth_string: str | None = None
    http_proxy: str | None = None
    proxy_file_path: str | None = None


@dataclass
class FileConfig:
    files: List[str] | None = None
    folder: str | None = None  # choose file_path or folder
    proxy_file_path: str | None = None


@dataclass
class FromConfig:
    chain: ChainType
    data_source: DataSource
    pool_address: str
    big_query: BigQueryConfig | None = None
    rpc: RpcConfig | None = None
    file: FileConfig | None = None


@dataclass
class TickConfig:
    get_position_id = False


@dataclass
class ToConfig:
    type: ToType  # minute or tick
    save_path: str
    tick_config: TickConfig


class OnchainTxType(str,Enum):
    MINT = "MINT"
    SWAP = "SWAP"
    BURN = "BURN"
    COLLECT = "COLLECT"


@dataclass
class Config:
    from_config: FromConfig
    to_config: ToConfig
