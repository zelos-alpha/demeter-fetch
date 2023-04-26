from datetime import date
from enum import Enum
from dataclasses import dataclass


class DataSource(Enum):
    big_query = 1
    rpc = 2
    file = 3


class ChainType(Enum):
    ethereum = 1
    polygon = 2
    optimism = 3
    arbitrum = 4
    celo = 5


class ToType(Enum):
    minute = 1
    tick = 2


@dataclass
class BigQueryConfig:
    start: date
    end: date
    auth_file: str


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
    file_path: str | None = None
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
class MinuteConfig:
    enable_proxy = False


@dataclass
class ToConfig:
    type: ToType  # minute or tick
    save_path: str
    minute_config: MinuteConfig

class OnchainTxType(Enum):
    MINT = 0
    SWAP = 2
    BURN = 1
    COLLECT = 3

@dataclass
class Config:
    from_config: FromConfig
    to_config: ToConfig
