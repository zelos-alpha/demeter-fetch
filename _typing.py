from datetime import date
from enum import Enum
from dataclasses import dataclass
from typing import List


class DataSource(str, Enum):
    big_query = "big_query"
    rpc = "rpc"
    file = "file"


class ChainType(str, Enum):
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


class ToType(str, Enum):
    minute = "minute"
    tick = "tick"
    raw = "raw"


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


class OnchainTxType(str, Enum):
    MINT = "MINT"
    SWAP = "SWAP"
    BURN = "BURN"
    COLLECT = "COLLECT"


@dataclass
class Config:
    from_config: FromConfig
    to_config: ToConfig


class MinuteData(object):

    def __init__(self):
        self.timestamp = None
        self.netAmount0 = 0
        self.netAmount1 = 0
        self.closeTick = None
        self.openTick = None
        self.lowestTick = None
        self.highestTick = None
        self.inAmount0 = 0
        self.inAmount1 = 0
        self.currentLiquidity = None

    def to_array(self):
        return [
            self.timestamp,
            self.netAmount0,
            self.netAmount1,
            self.closeTick,
            self.openTick,
            self.lowestTick,
            self.highestTick,
            self.inAmount0,
            self.inAmount1,
            self.currentLiquidity
        ]

    def __repr__(self):
        return str(self.timestamp)

    def __str__(self):
        return str(self.timestamp)

    def fill_missing_field(self, prev_data) -> bool:
        """
        fill missing field with previous data
        :param prev_data:
        :return: data is available or not
        """
        if prev_data is None:
            prev_data = MinuteData()
        self.closeTick = self.closeTick if self.closeTick is not None else prev_data.closeTick
        self.openTick = self.openTick if self.openTick is not None else prev_data.closeTick
        self.lowestTick = self.lowestTick if self.lowestTick is not None else prev_data.closeTick
        self.highestTick = self.highestTick if self.highestTick is not None else prev_data.closeTick
        self.currentLiquidity = self.currentLiquidity if self.currentLiquidity is not None \
            else prev_data.currentLiquidity

        return False if (self.closeTick is None or self.currentLiquidity is None) else True


MinuteDataNames = [
    "timestamp",
    "netAmount0",
    "netAmount1",
    "closeTick",
    "openTick",
    "lowestTick",
    "highestTick",
    "inAmount0",
    "inAmount1",
    "currentLiquidity",
]
