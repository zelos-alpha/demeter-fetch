import ast
import datetime
from typing import Dict, List

import numpy as np
import pandas as pd
from tqdm import tqdm

from demeter_fetch import NodeNames, GmxV2Config
from demeter_fetch.common import DailyNode, DailyParam, get_depend_name
from .gmx2_utils import GMX_FLOAT_DECIMAL

pool_file_columns = [
    "timestamp",  # 💚
    "block_number",  # 💚
    "transaction_Hash",  # 💚
    "tx_type",  # 💚
    "longAmount",  # 💚  #:  event MarketPoolValueInfo
    "shortAmount",  # 💚  #:  event MarketPoolValueInfo
    "virtualSwapInventoryLong",  # 💚   deposit,VirtualSwapInventoryUpdated, calculate priceImpact of deposit
    "virtualSwapInventoryShort",  # 💚 deposit,VirtualSwapInventoryUpdated
    "poolValue",  # 💚  deposit, event MarketPoolValueInfo,
    "marketTokensSupply",  # 💚  deposit, event MarketPoolValueInfo
    "impactPoolAmount",  # 💚  deposit, event MarketPoolValueInfo/PositionImpactPoolAmountUpdated
    "totalBorrowingFees",  # 💚
    "longPnl",  # 💚 from MarketPoolValueInfo, used to calculate poolvalue
    "shortPnl",  # 💚 from MarketPoolValueInfo
    "netPnl",  # 💚 from MarketPoolValueInfo
    "openInterestLongIsLong",  # 💚  event OpenInterestUpdated, to calculate pnl,
    "openInterestLongNotLong",  # 💚
    "openInterestShortIsLong",  # 💚
    "openInterestShortNotLong",  # 💚
    "openInterestInTokensLongIsLong",  # 💚   event : OpenInterestInTokensUpdated, to calculate pnl,
    "openInterestInTokensLongNotLong",  # 💚
    "openInterestInTokensShortIsLong",  # 💚
    "openInterestInTokensShortNotLong",  # 💚
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
        pool_snapshot["shortAmount"] = log_data["shortTokenAmount"] / 10**pool_config.short_token.decimal
        pool_snapshot["poolValue"] = log_data["poolValue"] / GMX_FLOAT_DECIMAL
        pool_snapshot["marketTokensSupply"] = log_data["marketTokensSupply"] / 10**18
        pool_snapshot["impactPoolAmount"] = log_data["impactPoolAmount"] / 10**pool_config.index_token.decimal
        pool_snapshot["totalBorrowingFees"] = log_data["totalBorrowingFees"] / GMX_FLOAT_DECIMAL
        pool_snapshot["longPnl"] = log_data["longPnl"] / GMX_FLOAT_DECIMAL
        pool_snapshot["shortPnl"] = log_data["shortPnl"] / GMX_FLOAT_DECIMAL
        pool_snapshot["netPnl"] = log_data["netPnl"] / GMX_FLOAT_DECIMAL


def _add_pool_value_prop_last(pool_config, tx_data, last_snapshot):
    # for the last row of the day
    log = find_log("MarketPoolValueUpdated", tx_data)
    if log is not None:
        log_data = ast.literal_eval(log["data"])
        last_snapshot["longAmount"] = log_data["longTokenAmount"] / 10**pool_config.long_token.decimal
        last_snapshot["shortAmount"] = log_data["shortTokenAmount"] / 10**pool_config.short_token.decimal
        last_snapshot["marketTokensSupply"] = log_data["marketTokensSupply"] / 10**18
        last_snapshot["impactPoolAmount"] = log_data["impactPoolAmount"] / 10**pool_config.index_token.decimal
        last_snapshot["totalBorrowingFees"] = log_data["totalBorrowingFees"] / GMX_FLOAT_DECIMAL


def _add_swap_inventory(pool_snapshot: Dict, pool_config, tx_data, last_snapshot):
    log = find_log("VirtualSwapInventoryUpdated", tx_data)
    if log is not None:
        log_data = ast.literal_eval(log["data"])
        old_val = log_data["nextValue"] - log_data["delta"]
        if log_data["isLongToken"]:
            pool_snapshot["virtualSwapInventoryLong"] = old_val / 10**pool_config.long_token.decimal
            last_snapshot["virtualSwapInventoryLong"] = log_data["nextValue"] / 10**pool_config.long_token.decimal
        else:
            pool_snapshot["virtualSwapInventoryShort"] = old_val / 10**pool_config.short_token.decimal
            last_snapshot["virtualSwapInventoryShort"] = log_data["nextValue"] / 10**pool_config.short_token.decimal


def _add_pool_amount(pool_snapshot: Dict, pool_config, tx_data, last_snapshot):
    log = find_log("PoolAmountUpdated", tx_data)
    if log is not None:
        log_data = ast.literal_eval(log["data"])
        old_val = log_data["nextValue"] - log_data["delta"]
        if log_data["token"].lower() == pool_config.long_token.address:
            pool_snapshot["longAmount"] = old_val / 10**pool_config.long_token.decimal
            last_snapshot["longAmount"] = log_data["nextValue"] / 10**pool_config.long_token.decimal
        else:
            pool_snapshot["shortAmount"] = old_val / 10**pool_config.short_token.decimal
            last_snapshot["shortAmount"] = log_data["nextValue"] / 10**pool_config.short_token.decimal


def _add_position_impact_pool_amount(pool_snapshot: Dict, pool_config, tx_data, last_snapshot):
    log = find_log("PositionImpactPoolAmountUpdated", tx_data)
    if log is not None:
        log_data = ast.literal_eval(log["data"])
        old_val = log_data["nextValue"] - log_data["delta"]
        pool_snapshot["impactPoolAmount"] = old_val / 10**pool_config.index_token.decimal
        last_snapshot["impactPoolAmount"] = log_data["nextValue"] / 10**pool_config.index_token.decimal


def _add_open_interest(pool_snapshot: Dict, pool_config: GmxV2Config, tx_data, last_snapshot):
    log = find_log("OpenInterestUpdated", tx_data)
    if log is not None:
        log_data = ast.literal_eval(log["data"])
        old_val = log_data["nextValue"] - log_data["delta"]
        if pool_config.long_token.address == log_data["collateralToken"]:
            if log_data["isLong"]:
                pool_snapshot["openInterestLongIsLong"] = old_val / GMX_FLOAT_DECIMAL
                last_snapshot["openInterestLongIsLong"] = log_data["nextValue"] / GMX_FLOAT_DECIMAL
            else:
                pool_snapshot["openInterestLongNotLong"] = old_val / GMX_FLOAT_DECIMAL
                last_snapshot["openInterestLongNotLong"] = log_data["nextValue"] / GMX_FLOAT_DECIMAL
        elif pool_config.short_token.address == log_data["collateralToken"]:
            if log_data["isLong"]:
                pool_snapshot["openInterestShortIsLong"] = old_val / GMX_FLOAT_DECIMAL
                last_snapshot["openInterestShortIsLong"] = log_data["nextValue"] / GMX_FLOAT_DECIMAL
            else:
                pool_snapshot["openInterestShortNotLong"] = old_val / GMX_FLOAT_DECIMAL
                last_snapshot["openInterestShortNotLong"] = log_data["nextValue"] / GMX_FLOAT_DECIMAL
        else:
            raise RuntimeError("OpenInterestUpdated should have long or short token")


def _add_open_interest_in_tokens(pool_snapshot: Dict, pool_config: GmxV2Config, tx_data, last_snapshot):
    log = find_log("OpenInterestInTokensUpdated", tx_data)
    if log is not None:
        log_data = ast.literal_eval(log["data"])
        old_val = log_data["nextValue"] - log_data["delta"]
        if pool_config.long_token.address == log_data["collateralToken"]:
            if log_data["isLong"]:
                pool_snapshot["openInterestInTokensLongIsLong"] = old_val / 10**pool_config.index_token.decimal
                last_snapshot["openInterestInTokensLongIsLong"] = (
                    log_data["nextValue"] / 10**pool_config.index_token.decimal
                )
            else:
                pool_snapshot["openInterestInTokensLongNotLong"] = old_val / 10**pool_config.index_token.decimal
                last_snapshot["openInterestInTokensLongNotLong"] = (
                    log_data["nextValue"] / 10**pool_config.index_token.decimal
                )
        elif pool_config.short_token.address == log_data["collateralToken"]:
            if log_data["isLong"]:
                pool_snapshot["openInterestInTokensShortIsLong"] = old_val / 10**pool_config.index_token.decimal
                last_snapshot["openInterestInTokensShortIsLong"] = (
                    log_data["nextValue"] / 10**pool_config.index_token.decimal
                )
            else:
                pool_snapshot["openInterestInTokensShortNotLong"] = old_val / 10**pool_config.index_token.decimal
                last_snapshot["openInterestInTokensShortNotLong"] = (
                    log_data["nextValue"] / 10**pool_config.index_token.decimal
                )
        else:
            raise RuntimeError("OpenInterestInTokensUpdated should have long or short token")


class GmxV2PoolTx(DailyNode):
    """
    Pool state when a transaction occurs.
    """
    def __init__(self):
        super().__init__()
        self.execute_in_sub_process = True


    name = NodeNames.gmx2_pool

    def _get_file_name(self, param: DailyParam) -> str:
        return (
            f"{self.from_config.chain.name}-GmxV2-{self.from_config.gmx_v2_config.GM_address}-{param.day.strftime('%Y-%m-%d')}.pool"
            + self._get_file_ext()
        )

    @property
    def _parse_date_column(self) -> List[str]:
        return ["timestamp"]

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: datetime.date) -> pd.DataFrame:
        """
        你需要使用这个交易开始的值, 这是为了让分钟级别数据更精确,
        比如, 交易在10:10:23发生, 而最终的分钟会需要10:10:0的状态,
        此时, 这个交易还没有发生, 因此你需要计算的是这个交易发生之前的状态.
        而10:11:0的数据, 会由后面的交易来填充. 这会造成的问题是这一天的末尾数据不对,
        比如, 最后一笔交易发生在23:50:25, 那么从23:51:00开始的数据, 肯定不能用23:50:00的数据, 因为这一分钟已经有交易改变状态了.
        因此设置一个last变量, 记录最后的状态, 并放在这一天的最后一刻. 这样简单的使用bfill就可以填充所有数据了


        You need to use the value at the start of each transaction to make minute-level data more accurate.
        For example, if a transaction occurs at 10:10:23, the final minute-level data will need the state at 10:10:00.
        At this point, the transaction hasn’t happened yet, so you need to calculate the state before this transaction occurs.
        The data for 10:11:00, on the other hand, will be filled by subsequent transactions.
        This can cause an issue where the data at the end of the day is incorrect.
        For instance, if the last transaction of the day happens at 23:50:25, the data starting from 23:51:00 cannot
        simply use the state at 23:50:00, because the state has already been altered by the transaction in that minute.
        Therefore, you should set a last variable to record the final state and place it at the very last moment of the day.
        This way, a simple bfill() can be used to fill all the data accurately.
        :param data:
        :param day:
        :return:
        """
        tick_df = data[get_depend_name(NodeNames.gmx2_tick, self.id)]

        pool_config = self.config.from_config.gmx_v2_config

        tick_df["market"] = tick_df["market"].str.lower()
        tick_df = tick_df[tick_df["market"] == pool_config.GM_address.lower()]
        tick_df = tick_df[(tick_df["tx_type"] != "") & (~tick_df["tx_type"].isna())]
        tick_df = tick_df.reset_index(drop=True)
        txes = tick_df.groupby(["block_number", "tx_index"])

        row_list = []
        # for the last row of the day
        last_snapshot = {
            "timestamp": datetime.datetime.combine(day, datetime.time(23, 59, 59), tzinfo=datetime.timezone.utc)
        }
        # all take the first value(before this tx happend)
        with tqdm(total=len(txes), ncols=60, position=1, leave=False) as pbar:
            for (height, tx_index), tx_data in txes:
                pool_snapshot = {
                    "timestamp": tx_data.iloc[0]["block_timestamp"],
                    "block_number": height,
                    "transaction_Hash": tx_data.iloc[0]["transaction_hash"],
                    "tx_type": tx_data.iloc[0]["tx_type"],
                }

                _add_pool_value_prop(pool_snapshot, pool_config, tx_data)
                _add_pool_value_prop_last(pool_config, tx_data, last_snapshot)
                _add_pool_amount(pool_snapshot, pool_config, tx_data, last_snapshot)
                _add_swap_inventory(pool_snapshot, pool_config, tx_data, last_snapshot)
                _add_open_interest(pool_snapshot, pool_config, tx_data, last_snapshot)
                _add_open_interest_in_tokens(pool_snapshot, pool_config, tx_data, last_snapshot)
                _add_position_impact_pool_amount(pool_snapshot, pool_config, tx_data, last_snapshot)
                row_list.append(pool_snapshot)
                pbar.update()

        row_list.append(last_snapshot)
        df = pd.DataFrame(row_list)

        for column_name in pool_file_columns:
            if column_name not in df.columns:
                df[column_name] = np.nan
        df = df[pool_file_columns]
        return df
