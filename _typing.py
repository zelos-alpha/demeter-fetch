from datetime import date
from enum import Enum
from dataclasses import dataclass
from typing import List


class DataSource(Enum):
    big_query = 1
    rpc = 2
    file = 3


class ChainType(Enum):
    ethereum = {"id": 1, "allow": [DataSource.big_query, DataSource.rpc, DataSource.file]}
    polygon = {"id": 2, "allow": [DataSource.big_query, DataSource.rpc, DataSource.file]}
    optimism = {"id": 3, "allow": [DataSource.rpc, DataSource.file]}
    arbitrum = {"id": 4, "allow": [DataSource.rpc, DataSource.file]}
    celo = {"id": 5, "allow": [DataSource.rpc, DataSource.file]}


class ToType(Enum):
    minute = 1
    tick = 2


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


class OnchainTxType(Enum):
    MINT = 0
    SWAP = 2
    BURN = 1
    COLLECT = 3


@dataclass
class Config:
    from_config: FromConfig
    to_config: ToConfig
