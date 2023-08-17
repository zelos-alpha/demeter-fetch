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
    bsc = "bsc"


# closest: 'before' or 'after'
ChainTypeConfig = {
    ChainType.ethereum: {
        "allow": [DataSource.big_query, DataSource.rpc, DataSource.file],
        "query_height_api": "https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp=%1&closest=%2",
        "proxy_addr": "0xc36442b4a4522e871399cd717abdd847ab11fe88",
    },
    ChainType.polygon: {
        "allow": [DataSource.big_query, DataSource.rpc, DataSource.file],
        "query_height_api": "https://api.polygonscan.com/api?module=block&action=getblocknobytime&timestamp=%1&closest=%2",
        "proxy_addr": "0xc36442b4a4522e871399cd717abdd847ab11fe88",
    },
    ChainType.optimism: {
        "allow": [DataSource.rpc, DataSource.file],
        "query_height_api": "https://api-optimistic.etherscan.io/api?module=block&action=getblocknobytime&timestamp=%1&closest=%2",
        "proxy_addr": "0xc36442b4a4522e871399cd717abdd847ab11fe88",
    },
    ChainType.arbitrum: {
        "allow": [DataSource.rpc, DataSource.file],
        "query_height_api": "https://api.arbiscan.io/api?module=block&action=getblocknobytime&timestamp=%1&closest=%2",
        "proxy_addr": "0xc36442b4a4522e871399cd717abdd847ab11fe88",
    },
    ChainType.celo: {
        "allow": [DataSource.rpc, DataSource.file],
        "query_height_api": "https://api.celoscan.io/api?module=block&action=getblocknobytime&timestamp=%1&closest=%2",
        "proxy_addr": "0x3d79edaabc0eab6f08ed885c05fc0b014290d95a",
    },
    ChainType.bsc: {
        "allow": [DataSource.rpc, DataSource.file],
        "query_height_api": "https://api.bscscan.com/api?module=block&action=getblocknobytime&timestamp=%1&closest=%2",
        "proxy_addr": "0x7b8a01b39d58278b5de7e48c8449c9f4f5170613",
    },
}


class ToType(str, Enum):
    minute = "minute"
    tick = "tick"
    raw = "raw"


class DappType(str, Enum):
    uniswap = "uniswap"
    aave = "aave"


@dataclass
class BigQueryConfig:
    start: date
    end: date
    auth_file: str
    http_proxy: str | None = None


@dataclass
class RpcConfig:
    end_point: str
    start: date
    end: date
    batch_size: int = 500
    auth_string: str | None = None
    http_proxy: str | None = None
    keep_tmp_files: bool = False
    ignore_position_id: bool = (
        False  # if set to true, will not download proxy logs and leave a empty column
    )


@dataclass
class FileConfig:
    files: List[str] | None = None
    folder: str | None = None  # choose file_path or folder


@dataclass
class UniswapConfig:
    pool_address: str


@dataclass
class AaveConfig:
    tokens: List[str]


@dataclass
class FromConfig:
    chain: ChainType
    data_source: DataSource
    dapp_type: DappType
    uniswap_config: UniswapConfig | None = None
    aave_config: AaveConfig | None = None
    big_query: BigQueryConfig | None = None
    rpc: RpcConfig | None = None
    file: FileConfig | None = None


@dataclass
class ToConfig:
    type: ToType  # minute or tick
    save_path: str
    multi_process: bool


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
            self.currentLiquidity,
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
        self.closeTick = (
            self.closeTick if self.closeTick is not None else prev_data.closeTick
        )
        self.openTick = (
            self.openTick if self.openTick is not None else prev_data.closeTick
        )
        self.lowestTick = (
            self.lowestTick if self.lowestTick is not None else prev_data.closeTick
        )
        self.highestTick = (
            self.highestTick if self.highestTick is not None else prev_data.closeTick
        )
        self.currentLiquidity = (
            self.currentLiquidity
            if self.currentLiquidity is not None
            else prev_data.currentLiquidity
        )

        return (
            False if (self.closeTick is None or self.currentLiquidity is None) else True
        )


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


class EthError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message
