import ast
import datetime
from datetime import date
from typing import Dict

import pandas as pd
from eth_abi import decode
from tqdm import tqdm

from demeter_fetch import NodeNames, GmxV2Config
from demeter_fetch.common import DailyNode, DailyParam, get_depend_name
from .gmx2_utils import data_type, data_decoder, GMX_FLOAT_DECIMAL

minute_file_columns = [
    "timestamp",  # 💚
    "longAmount",  # 💚  #: int  # event MarketPoolValueInfo
    "shortAmount",  # 💚  #: int  # event MarketPoolValueInfo
    "virtualSwapInventoryLong",  # 💚  #: int  # deposit,VirtualSwapInventoryUpdated, 计算priceImpact用
    "virtualSwapInventoryShort",  # 💚  #: int  # deposit,VirtualSwapInventoryUpdated
    "poolValue",  # 💛  #: int  # deposit, event MarketPoolValueInfo, # 校对poolValue,
    "marketTokensSupply",  # 💚  #: int  # deposit, event MarketPoolValueInfo
    # "longPrice",  # 💢  #: Prices  # deposit, event OraclePriceUpdate # 价格非常有用
    # "shortPrice",  # 💢  #: Prices  # deposit, event OraclePriceUpdate #
    # "indexPrice",  # 💢  #: Prices | None = None  # event OraclePriceUpdate
    "impactPoolAmount",  # 💛  #: int  # deposit, event MarketPoolValueInfo/PositionImpactPoolAmountUpdated
    "totalBorrowingFees",  # 💛
    "longPnl",  # 💛
    "shortPnl",  # 💛
    "netPnl",  # 💛
    "openInterestLongIsLong",  # 💚  #: int = 0  # event OpenInterestUpdated, 估算pnl以估算poolvalue
    "openInterestLongNotLong",  # 💚  #: int = 0
    "openInterestShortIsLong",  # 💚  #: int = 0
    "openInterestShortNotLong",  # 💚  #: int = 0
    "openInterestInTokensLongIsLong",  # 💚  #: int = 0  # event : OpenInterestInTokensUpdated, 估算pnl以估算poolvalue
    "openInterestInTokensLongNotLong",  # 💚  #: int = 0
    "openInterestInTokensShortIsLong",  # 💚  #: int = 0
    "openInterestInTokensShortNotLong",  # 💚  #: int = 0
    "positionFeesCollected", # 💚
]


def find_log(name: str, tx_data: pd.DataFrame) -> pd.Series | None:
    # Grok give me this magic line. it believes it is the fastest to find first record
    locate = (tx_data["event_name"] == name).idxmax()
    if locate > tx_data.index[0]:
        return tx_data.loc[locate]
    if tx_data.iloc[0]["event_name"] == name:
        return tx_data.iloc[0]
    else:
        return None


def _add_pool_value_prop(pool_snapshot: Dict, pool_config, tx_data):
    log = find_log("MarketPoolValueInfo", tx_data)
    if log is not None:
        log_data = ast.literal_eval(log["data"])
        pool_snapshot["longAmount"] = log_data["longTokenAmount"] / 10**pool_config.long_token.decimal
        pool_snapshot["shortAmount"] = log_data["shortTokenAmount"] / 10**pool_config.long_token.decimal
        pool_snapshot["poolValue"] = log_data["poolValue"] / GMX_FLOAT_DECIMAL
        pool_snapshot["marketTokensSupply"] = log_data["marketTokensSupply"] / 10**18
        pool_snapshot["impactPoolAmount"] = log_data["impactPoolAmount"] / 10**pool_config.index_token.decimal
        pool_snapshot["totalBorrowingFees"] = log_data["totalBorrowingFees"] / GMX_FLOAT_DECIMAL
        pool_snapshot["longPnl"] = log_data["longPnl"] / GMX_FLOAT_DECIMAL
        pool_snapshot["shortPnl"] = log_data["shortPnl"] / GMX_FLOAT_DECIMAL
        pool_snapshot["netPnl"] = log_data["netPnl"] / GMX_FLOAT_DECIMAL


def _add_swap_inventory(pool_snapshot: Dict, pool_config, tx_data):
    log = find_log("VirtualSwapInventoryUpdated", tx_data)
    if log is not None:
        log_data = ast.literal_eval(log["data"])
        old_val = log_data["nextValue"] - log_data["delta"]
        if log_data["isLongToken"]:
            pool_snapshot["virtualSwapInventoryLong"] = old_val / 10**pool_config.long_token.decimal
        else:
            pool_snapshot["virtualSwapInventoryShort"] = old_val / 10**pool_config.short_token.decimal

def _add_position_fees(pool_snapshot: Dict, pool_config, tx_data):
    log = find_log("PositionFeesCollected", tx_data)
    if log is not None:
        log_data = ast.literal_eval(log["data"])
        old_val = log_data["nextValue"] - log_data["delta"]
        if log_data["isLongToken"]:
            pool_snapshot["virtualSwapInventoryLong"] = old_val / 10**pool_config.long_token.decimal
        else:
            pool_snapshot["virtualSwapInventoryShort"] = old_val / 10**pool_config.short_token.decimal



def _add_open_interest(pool_snapshot: Dict, pool_config: GmxV2Config, tx_data):
    log = find_log("OpenInterestUpdated", tx_data)
    if log is not None:
        log_data = ast.literal_eval(log["data"])
        old_val = log_data["nextValue"] - log_data["delta"]
        if pool_config.long_token.address == log_data["collateralToken"]:
            if log_data["isLong"]:
                pool_snapshot["openInterestLongIsLong"] = old_val / GMX_FLOAT_DECIMAL
            else:
                pool_snapshot["openInterestLongNotLong"] = old_val / GMX_FLOAT_DECIMAL
        elif pool_config.short_token.address == log_data["collateralToken"]:
            if log_data["isLong"]:
                pool_snapshot["openInterestShortIsLong"] = old_val / GMX_FLOAT_DECIMAL
            else:
                pool_snapshot["openInterestShortNotLong"] = old_val / GMX_FLOAT_DECIMAL
        else:
            raise RuntimeError("OpenInterestUpdated should have long or short token")


def _add_open_interest_in_tokens(pool_snapshot: Dict, pool_config: GmxV2Config, tx_data):
    log = find_log("OpenInterestInTokensUpdated", tx_data)
    if log is not None:
        log_data = ast.literal_eval(log["data"])
        old_val = log_data["nextValue"] - log_data["delta"]
        if pool_config.long_token.address == log_data["collateralToken"]:
            if log_data["isLong"]:
                pool_snapshot["openInterestInTokensLongIsLong"] = old_val / 10**pool_config.index_token.decimal
            else:
                pool_snapshot["openInterestInTokensLongNotLong"] = old_val / 10**pool_config.index_token.decimal
        elif pool_config.short_token.address == log_data["collateralToken"]:
            if log_data["isLong"]:
                pool_snapshot["openInterestInTokensShortIsLong"] = old_val / 10**pool_config.index_token.decimal
            else:
                pool_snapshot["openInterestInTokensShortNotLong"] = old_val / 10**pool_config.index_token.decimal
        else:
            raise RuntimeError("OpenInterestInTokensUpdated should have long or short token")


class GmxV2PoolTx(DailyNode):
    name = NodeNames.gmx2_pool

    def _get_file_name(self, param: DailyParam) -> str:
        return (
            f"{self.from_config.chain.name}-GmxV2-{self.from_config.gmx_v2_config.GM_address}-{param.day.strftime('%Y-%m-%d')}.pool"
            + self._get_file_ext()
        )

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: datetime.date) -> pd.DataFrame:
        tick_df = data[get_depend_name(NodeNames.gmx2_tick, self.id)]

        pool_config = self.config.from_config.gmx_v2_config

        tick_df = tick_df[tick_df["market"] == pool_config.GM_address]
        tick_df = tick_df[tick_df["tx_type"] != ""]
        tick_df = tick_df.reset_index(drop=True)
        txes = tick_df.groupby(["block_number", "tx_index"])

        row_list = []
        # all take the first value(before this tx happend)
        with tqdm(total=len(txes), ncols=60, position=1, leave=False) as pbar:
            for (height, tx_index), tx_data in txes:
                pool_snapshot = {"timestamp": tx_data.iloc[0]["block_timestamp"]}
                _add_pool_value_prop(pool_snapshot, pool_config, tx_data)
                _add_swap_inventory(pool_snapshot, pool_config, tx_data)
                _add_open_interest(pool_snapshot, pool_config, tx_data)
                _add_open_interest_in_tokens(pool_snapshot, pool_config, tx_data)
                row_list.append(pool_snapshot)
                pbar.update()

        df = pd.DataFrame(row_list)
        df = df[minute_file_columns]
        return df


class GmxV2Minute(DailyNode):
    name = NodeNames.gmx2_minute

    def _get_file_name(self, param: DailyParam) -> str:
        return (
            f"{self.from_config.chain.name}-GmxV2-{self.from_config.gmx_v2_config.GM_address}-{param.day.strftime('%Y-%m-%d')}.minute"
            + self._get_file_ext()
        )
