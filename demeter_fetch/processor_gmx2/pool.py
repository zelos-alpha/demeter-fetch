import ast
import datetime
from typing import Dict, List, NamedTuple

import numpy as np
import pandas as pd
from tqdm import tqdm

from .. import NodeNames
from ..common import DailyNode, DailyParam, get_depend_name
from .gmx2_utils import GMX_FLOAT_DECIMAL, SwapFeeType, GM_DECIMAL, GMX_FLOAT_PRECISION_SQRT


class PoolInfo(NamedTuple):
    long_decimal: int
    short_decimal: int
    index_decimal: int
    long_addr: str
    short_addr: str
    index_addr: str


pool_file_columns = [
    "timestamp",  # 💚
    "block_number",  # 💚
    "transaction_Hash",  # 💚
    "tx_type",  # 💚
    "longAmount",  # 💚  #:  event MarketPoolValueInfo
    "shortAmount",  # 💚  #:  event MarketPoolValueInfo
    "longAmountDelta",  # 💚
    "shortAmountDelta",  # 💚
    "longAmountDeltaNoFee",  # 💚
    "shortAmountDeltaNoFee",  # 💚
    "virtualSwapInventoryLong",  # 💚   deposit,VirtualSwapInventoryUpdated, calculate priceImpact of deposit
    "virtualSwapInventoryShort",  # 💚 deposit,VirtualSwapInventoryUpdated
    "poolValue",  # 💚  deposit, event MarketPoolValueInfo,
    "marketTokensSupply",  # 💚  deposit, event MarketPoolValueInfo
    "impactPoolAmount",  # 💚  deposit, event MarketPoolValueInfo/PositionImpactPoolAmountUpdated
    "totalBorrowingFees",  # 💚 in pool, event MarketPoolValueInfo
    "borrowingFeePoolFactor",  # 💚in pool, event MarketPoolValueInfo
    "borrowingFeeUsd",  # 💚 for each position
    "longPnl",  # 💚 from MarketPoolValueInfo, used to calculate poolvalue
    "shortPnl",  # 💚 from MarketPoolValueInfo
    "netPnl",  # 💚 from MarketPoolValueInfo
    "realizedPnl",
    "LongTokenOpenInterestLong",  # 💚  event OpenInterestUpdated, to calculate pnl,
    "LongTokenOpenInterestShort",  # 💚
    "ShortTokenOpenInterestLong",  # 💚
    "ShortTokenOpenInterestShort",  # 💚
    "LongTokenOpenInterestInTokensLong",  # 💚   event : OpenInterestInTokensUpdated, to calculate pnl,
    "LongTokenOpenInterestInTokensShort",  # 💚
    "ShortTokenOpenInterestInTokensLong",  # 💚
    "ShortTokenOpenInterestInTokensShort",  # 💚
    "virtualPositionInventory",
    "cumulativeBorrowingFactorLong",
    "cumulativeBorrowingFactorShort",
    "longTokenFundingFeeAmountPerSizeLong",
    "longTokenFundingFeeAmountPerSizeShort",
    "shortTokenFundingFeeAmountPerSizeLong",
    "shortTokenFundingFeeAmountPerSizeShort",
    "longTokenClaimableFundingAmountPerSizeLong",
    "longTokenClaimableFundingAmountPerSizeShort",
    "shortTokenClaimableFundingAmountPerSizeLong",
    "shortTokenClaimableFundingAmountPerSizeShort",
    "positionImpactPoolAmount",
]


def find_logs(name: str, tx_data: pd.DataFrame) -> pd.Series | None:
    # Grok give me this magic line. it believes it is the fastest to find first record
    # locate = (tx_data["event_name"] == name).idxmax()
    # if locate > tx_data.index[0]:
    #     return tx_data.loc[locate]
    # if tx_data.iloc[0]["event_name"] == name:
    #     return tx_data.iloc[0]
    # else:
    #     return None
    return tx_data[tx_data["event_name"] == name]


def _add_pool_value_prop(pool_snapshot: Dict, pool_info: PoolInfo, tx_data):
    logs = find_logs("MarketPoolValueInfo", tx_data)
    logs = logs.head(1)  # just use the first one
    for idx, log in logs.iterrows():
        log_data = ast.literal_eval(log["data"])
        pool_snapshot["longAmount"] = log_data["longTokenAmount"] / pool_info.long_decimal
        pool_snapshot["shortAmount"] = log_data["shortTokenAmount"] / pool_info.short_decimal
        if log_data["shortTokenAmount"] / pool_info.short_decimal > 70441588600579:
            print("_add_pool_value_prop", log_data["shortTokenAmount"] / pool_info.short_decimal)
        pool_snapshot["poolValue"] = log_data["poolValue"] / GMX_FLOAT_DECIMAL
        pool_snapshot["marketTokensSupply"] = log_data["marketTokensSupply"] / GM_DECIMAL
        pool_snapshot["impactPoolAmount"] = log_data["impactPoolAmount"] / pool_info.index_decimal
        pool_snapshot["totalBorrowingFees"] = log_data["totalBorrowingFees"] / GMX_FLOAT_DECIMAL
        pool_snapshot["longPnl"] = log_data["longPnl"] / GMX_FLOAT_DECIMAL
        pool_snapshot["shortPnl"] = log_data["shortPnl"] / GMX_FLOAT_DECIMAL
        pool_snapshot["netPnl"] = log_data["netPnl"] / GMX_FLOAT_DECIMAL
        pool_snapshot["borrowingFeePoolFactor"] = log_data["borrowingFeePoolFactor"] / GMX_FLOAT_DECIMAL


def _add_pool_value_prop_last(pool_info: PoolInfo, tx_data, last_snapshot):
    # for the last row of the day
    logs = find_logs("MarketPoolValueUpdated", tx_data)
    logs = logs.tail(1)  # use the last one
    for idx, log in logs.iterrows():
        log_data = ast.literal_eval(log["data"])
        last_snapshot["longAmount"] = log_data["longTokenAmount"] / pool_info.long_decimal
        last_snapshot["shortAmount"] = log_data["shortTokenAmount"] / pool_info.short_decimal
        if log_data["shortTokenAmount"] / pool_info.short_decimal > 70441588600579:
            print("_add_pool_value_prop_last", log_data["shortTokenAmount"] / pool_info.short_decimal)
        last_snapshot["marketTokensSupply"] = log_data["marketTokensSupply"] / GM_DECIMAL
        last_snapshot["impactPoolAmount"] = log_data["impactPoolAmount"] / pool_info.index_decimal
        last_snapshot["totalBorrowingFees"] = log_data["totalBorrowingFees"] / GMX_FLOAT_DECIMAL


def _add_swap_delta_in_swap(pool_snapshot: Dict, pool_info: PoolInfo, tx_data):
    logs = find_logs("SwapInfo", tx_data)
    for idx, log in logs.iterrows():
        log_data = ast.literal_eval(log["data"])
        if log_data["tokenIn"].lower() == pool_info.long_addr:
            pool_snapshot["longAmountDeltaNoFee"] += log_data["amountInAfterFees"] / pool_info.long_decimal
            pool_snapshot["shortAmountDeltaNoFee"] -= log_data["amountOut"] / pool_info.short_decimal
        elif log_data["tokenIn"].lower() == pool_info.short_addr:
            pool_snapshot["shortAmountDeltaNoFee"] += log_data["amountInAfterFees"] / pool_info.short_decimal
            pool_snapshot["longAmountDeltaNoFee"] -= log_data["amountOut"] / pool_info.long_decimal


def _add_swap_delta_in_deposits(pool_snapshot: Dict, pool_info: PoolInfo, tx_data):
    # for the last row of the day
    logs = find_logs("SwapFeesCollected", tx_data)
    for idx, log in logs.iterrows():
        log_data = ast.literal_eval(log["data"])
        # just process swap fees of deposit and withdraw,
        swap_fee_type = "0x" + log_data["swapFeeType"].hex()
        if swap_fee_type == SwapFeeType.Deposit.value:
            value = log_data["amountAfterFees"]
        elif swap_fee_type == SwapFeeType.Withdraw.value:
            value = -log_data["amountAfterFees"]
        else:
            continue

        if log_data["token"].lower() == pool_info.long_addr:
            pool_snapshot["longAmountDeltaNoFee"] += value / pool_info.long_decimal
        else:
            pool_snapshot["shortAmountDeltaNoFee"] += value / pool_info.short_decimal


def _add_virtual_swap_inventory(pool_snapshot: Dict, pool_info: PoolInfo, tx_data, last_snapshot):
    logs = find_logs("VirtualSwapInventoryUpdated", tx_data)
    # Note: VirtualSwapInventory is updated by all relative pool, although we can filter every log to get a accurate value,
    # But this value doesn't have to be accurate. because it is used in calculation of position price impact.
    # and it will affect only when virtual price impact < pool price impact.
    # More important, we can get accurate value when it is updated.
    long_list = []
    short_list = []
    for idx, log in logs.iterrows():
        log_data = ast.literal_eval(log["data"])
        old_val = log_data["nextValue"] - log_data["delta"]
        # get amount
        if log_data["isLongToken"]:
            long_list.append((old_val, log_data["nextValue"]))
        else:
            short_list.append((old_val, log_data["nextValue"]))
    if len(long_list) > 0:
        pool_snapshot["virtualSwapInventoryLong"] = long_list[0][0] / pool_info.long_decimal
        last_snapshot["virtualSwapInventoryLong"] = long_list[-1][1] / pool_info.long_decimal
    if len(short_list) > 0:
        pool_snapshot["virtualSwapInventoryShort"] = short_list[0][0] / pool_info.short_decimal
        last_snapshot["virtualSwapInventoryShort"] = short_list[-1][1] / pool_info.short_decimal


def _add_virtual_position_inventory(pool_snapshot: Dict, pool_info: PoolInfo, log, last_snapshot):
    log_data = ast.literal_eval(log["data"])
    old_val = log_data["nextValue"] - log_data["delta"]
    # get amount
    if log_data["token"] == pool_info.index_addr:
        pool_snapshot["virtualPositionInventory"] = old_val / GMX_FLOAT_DECIMAL
        last_snapshot["virtualPositionInventory"] = log_data["nextValue"] / GMX_FLOAT_DECIMAL
    else:
        raise RuntimeError("VirtualPositionInventoryUpdated should have token")


def _add_cumulative_borrowing_factor(pool_snapshot: Dict, pool_info: PoolInfo, tx_data, last_snapshot):
    logs = find_logs("CumulativeBorrowingFactorUpdated", tx_data)
    long_list = []
    short_list = []
    for idx, log in logs.iterrows():
        log_data = ast.literal_eval(log["data"])
        old_val = log_data["nextValue"] - log_data["delta"]
        # get amount
        if log_data["isLong"]:
            long_list.append((old_val, log_data["nextValue"]))
        else:
            short_list.append((old_val, log_data["nextValue"]))
    if len(long_list) > 0:
        pool_snapshot["cumulativeBorrowingFactorLong"] = long_list[0][0] / GMX_FLOAT_DECIMAL
        last_snapshot["cumulativeBorrowingFactorLong"] = long_list[-1][1] / GMX_FLOAT_DECIMAL
    if len(short_list) > 0:
        pool_snapshot["cumulativeBorrowingFactorShort"] = short_list[0][0] / GMX_FLOAT_DECIMAL
        last_snapshot["cumulativeBorrowingFactorShort"] = short_list[-1][1] / GMX_FLOAT_DECIMAL


def _add_unchanged_funding_fee_per_size(pool_snapshot: Dict, pool_info: PoolInfo, tx_data, last_snapshot):
    logs = find_logs("PositionFeesCollected", tx_data)
    for idx, log in logs.iterrows():
        log_data = ast.literal_eval(log["data"])
        next_log = tx_data.loc[idx + 1]
        # according to contract code, PositionDecrease is just after position fee,
        # find position info to get "isLong"
        assert next_log["event_name"] in ["PositionDecrease", "PositionIncrease"]
        next_log_data = ast.literal_eval(next_log["data"])
        is_long = next_log_data["isLong"]

        latestFundingFeeAmountPerSize = log_data["latestFundingFeeAmountPerSize"] / GMX_FLOAT_PRECISION_SQRT
        if pool_info.long_addr == log_data["collateralToken"]:
            latestFundingFeeAmountPerSize /= pool_info.long_decimal
            if is_long:
                pool_snapshot["longTokenFundingFeeAmountPerSizeLong"] = latestFundingFeeAmountPerSize
                last_snapshot["longTokenFundingFeeAmountPerSizeLong"] = latestFundingFeeAmountPerSize
            else:
                pool_snapshot["longTokenFundingFeeAmountPerSizeShort"] = latestFundingFeeAmountPerSize
                last_snapshot["longTokenFundingFeeAmountPerSizeShort"] = latestFundingFeeAmountPerSize
        elif pool_info.short_addr == log_data["collateralToken"]:
            latestFundingFeeAmountPerSize /= pool_info.short_decimal
            if is_long:
                pool_snapshot["shortTokenFundingFeeAmountPerSizeLong"] = latestFundingFeeAmountPerSize
                last_snapshot["shortTokenFundingFeeAmountPerSizeLong"] = latestFundingFeeAmountPerSize
            else:
                pool_snapshot["shortTokenFundingFeeAmountPerSizeShort"] = latestFundingFeeAmountPerSize
                last_snapshot["shortTokenFundingFeeAmountPerSizeShort"] = latestFundingFeeAmountPerSize
        else:
            raise RuntimeError("FundingFeeAmountPerSizeUpdated should have long or short token")

        longTokenClaimableFundingAmountPerSize = (
            log_data["latestLongTokenClaimableFundingAmountPerSize"] / GMX_FLOAT_PRECISION_SQRT / pool_info.long_decimal
        )
        shortTokenClaimableFundingAmountPerSize = (
            log_data["latestShortTokenClaimableFundingAmountPerSize"]
            / GMX_FLOAT_PRECISION_SQRT
            / pool_info.short_decimal
        )
        if is_long:
            pool_snapshot["longTokenClaimableFundingAmountPerSizeLong"] = longTokenClaimableFundingAmountPerSize
            last_snapshot["longTokenClaimableFundingAmountPerSizeLong"] = longTokenClaimableFundingAmountPerSize
            pool_snapshot["shortTokenClaimableFundingAmountPerSizeLong"] = shortTokenClaimableFundingAmountPerSize
            last_snapshot["shortTokenClaimableFundingAmountPerSizeLong"] = shortTokenClaimableFundingAmountPerSize
        else:
            pool_snapshot["longTokenClaimableFundingAmountPerSizeShort"] = longTokenClaimableFundingAmountPerSize
            last_snapshot["longTokenClaimableFundingAmountPerSizeShort"] = longTokenClaimableFundingAmountPerSize
            pool_snapshot["shortTokenClaimableFundingAmountPerSizeShort"] = shortTokenClaimableFundingAmountPerSize
            last_snapshot["shortTokenClaimableFundingAmountPerSizeShort"] = shortTokenClaimableFundingAmountPerSize


def _add_funding_fee_amount_per_size(pool_snapshot: Dict, pool_info: PoolInfo, tx_data, last_snapshot):
    logs = find_logs("FundingFeeAmountPerSizeUpdated", tx_data)
    for idx, log in logs.iterrows():
        log_data = ast.literal_eval(log["data"])
        old_val = log_data["value"] - log_data["delta"]
        if pool_info.long_addr == log_data["collateralToken"]:
            if log_data["isLong"]:
                pool_snapshot["longTokenFundingFeeAmountPerSizeLong"] = (
                    old_val / GMX_FLOAT_PRECISION_SQRT / pool_info.long_decimal
                )
                last_snapshot["longTokenFundingFeeAmountPerSizeLong"] = (
                    log_data["value"] / GMX_FLOAT_PRECISION_SQRT / pool_info.long_decimal
                )
            else:
                pool_snapshot["longTokenFundingFeeAmountPerSizeShort"] = (
                    old_val / GMX_FLOAT_PRECISION_SQRT / pool_info.long_decimal
                )
                last_snapshot["longTokenFundingFeeAmountPerSizeShort"] = (
                    log_data["value"] / GMX_FLOAT_PRECISION_SQRT / pool_info.long_decimal
                )
        elif pool_info.short_addr == log_data["collateralToken"]:
            if log_data["isLong"]:
                pool_snapshot["shortTokenFundingFeeAmountPerSizeLong"] = (
                    old_val / GMX_FLOAT_PRECISION_SQRT / pool_info.short_decimal
                )
                last_snapshot["shortTokenFundingFeeAmountPerSizeLong"] = (
                    log_data["value"] / GMX_FLOAT_PRECISION_SQRT / pool_info.short_decimal
                )
            else:
                pool_snapshot["shortTokenFundingFeeAmountPerSizeShort"] = (
                    old_val / GMX_FLOAT_PRECISION_SQRT / pool_info.short_decimal
                )
                last_snapshot["shortTokenFundingFeeAmountPerSizeShort"] = (
                    log_data["value"] / GMX_FLOAT_PRECISION_SQRT / pool_info.short_decimal
                )
        else:
            raise RuntimeError("FundingFeeAmountPerSizeUpdated should have long or short token")


def _add_claimable_funding_amount_per_size(pool_snapshot: Dict, pool_info: PoolInfo, tx_data, last_snapshot):
    logs = find_logs("ClaimableFundingAmountPerSizeUpdated", tx_data)
    for idx, log in logs.iterrows():
        log_data = ast.literal_eval(log["data"])
        old_val = log_data["value"] - log_data["delta"]
        if pool_info.long_addr == log_data["collateralToken"]:
            if log_data["isLong"]:
                pool_snapshot["longTokenClaimableFundingAmountPerSizeLong"] = (
                    old_val / GMX_FLOAT_PRECISION_SQRT / pool_info.long_decimal
                )
                last_snapshot["longTokenClaimableFundingAmountPerSizeLong"] = (
                    log_data["value"] / GMX_FLOAT_PRECISION_SQRT / pool_info.long_decimal
                )
            else:
                pool_snapshot["longTokenClaimableFundingAmountPerSizeShort"] = (
                    old_val / GMX_FLOAT_PRECISION_SQRT / pool_info.long_decimal
                )
                last_snapshot["longTokenClaimableFundingAmountPerSizeShort"] = (
                    log_data["value"] / GMX_FLOAT_PRECISION_SQRT / pool_info.long_decimal
                )
        elif pool_info.short_addr == log_data["collateralToken"]:
            if log_data["isLong"]:
                pool_snapshot["shortTokenClaimableFundingAmountPerSizeLong"] = (
                    old_val / GMX_FLOAT_PRECISION_SQRT / pool_info.short_decimal
                )
                last_snapshot["shortTokenClaimableFundingAmountPerSizeLong"] = (
                    log_data["value"] / GMX_FLOAT_PRECISION_SQRT / pool_info.short_decimal
                )
            else:
                pool_snapshot["shortTokenClaimableFundingAmountPerSizeShort"] = (
                    old_val / GMX_FLOAT_PRECISION_SQRT / pool_info.short_decimal
                )
                last_snapshot["shortTokenClaimableFundingAmountPerSizeShort"] = (
                    log_data["value"] / GMX_FLOAT_PRECISION_SQRT / pool_info.short_decimal
                )
        else:
            raise RuntimeError("ClaimableFundingAmountPerSizeUpdated should have long or short token")


def _add_pool_amount_updated(pool_snapshot: Dict, pool_info: PoolInfo, tx_data, last_snapshot):
    logs = find_logs("PoolAmountUpdated", tx_data)
    # tx_hash = tx_data.iloc[0]["transaction_hash"]

    long_list = []
    short_list = []
    for idx, log in logs.iterrows():
        log_data = ast.literal_eval(log["data"])
        old_val = log_data["nextValue"] - log_data["delta"]
        # get delta, and amount
        if pool_info.long_addr != pool_info.short_addr:
            if log_data["token"].lower() == pool_info.long_addr:
                pool_snapshot["longAmountDelta"] += log_data["delta"] / pool_info.long_decimal
                long_list.append((old_val, log_data["nextValue"]))
            else:
                pool_snapshot["shortAmountDelta"] += log_data["delta"] / pool_info.short_decimal
                short_list.append((old_val, log_data["nextValue"]))
        else:
            delta = log_data["delta"] / 2
            old_val = old_val / 2
            next_val = log_data["nextValue"] / 2
            pool_snapshot["longAmountDelta"] += delta / pool_info.long_decimal
            long_list.append((old_val, next_val))
            pool_snapshot["shortAmountDelta"] += delta / pool_info.short_decimal
            short_list.append((old_val, next_val))
    if len(long_list) > 0:
        pool_snapshot["longAmount"] = long_list[0][0] / pool_info.long_decimal
        last_snapshot["longAmount"] = long_list[-1][1] / pool_info.long_decimal
    if len(short_list) > 0:
        pool_snapshot["shortAmount"] = short_list[0][0] / pool_info.short_decimal
        last_snapshot["shortAmount"] = short_list[-1][1] / pool_info.short_decimal
        if short_list[0][0] / pool_info.short_decimal / pool_info.short_decimal > 70441588600579:
            print("_add_pool_amount_updated", short_list[0][0] / pool_info.short_decimal)


def _add_position_impact_pool_amount(pool_snapshot: Dict, pool_info: PoolInfo, tx_data, last_snapshot):
    logs = find_logs("PositionImpactPoolAmountUpdated", tx_data)
    for idx, log in logs.iterrows():
        log_data = ast.literal_eval(log["data"])
        old_val = log_data["nextValue"] - log_data["delta"]
        pool_snapshot["impactPoolAmount"] = old_val / pool_info.index_decimal
        last_snapshot["impactPoolAmount"] = log_data["nextValue"] / pool_info.index_decimal


def _add_position_impact_pool_amount_v2(pool_snapshot: Dict, pool_info: PoolInfo, tx_data, last_snapshot):
    logs = find_logs("PositionImpactPoolAmountUpdated", tx_data)
    for idx, log in logs.iterrows():
        log_data = ast.literal_eval(log["data"])
        old_val = log_data["nextValue"] - log_data["delta"]
        pool_snapshot["positionImpactPoolAmount"] = old_val / pool_info.index_decimal
        last_snapshot["positionImpactPoolAmount"] = log_data["nextValue"] / pool_info.index_decimal


def _add_open_interest(pool_snapshot: Dict, pool_info: PoolInfo, tx_data, last_snapshot):
    logs = find_logs("OpenInterestUpdated", tx_data)
    for idx, log in logs.iterrows():
        log_data = ast.literal_eval(log["data"])
        old_val = log_data["nextValue"] - log_data["delta"]
        if pool_info.long_addr == log_data["collateralToken"]:
            if log_data["isLong"]:
                pool_snapshot["LongTokenOpenInterestLong"] = old_val / GMX_FLOAT_DECIMAL
                last_snapshot["LongTokenOpenInterestLong"] = log_data["nextValue"] / GMX_FLOAT_DECIMAL
            else:
                pool_snapshot["LongTokenOpenInterestShort"] = old_val / GMX_FLOAT_DECIMAL
                last_snapshot["LongTokenOpenInterestShort"] = log_data["nextValue"] / GMX_FLOAT_DECIMAL
        elif pool_info.short_addr == log_data["collateralToken"]:
            if log_data["isLong"]:
                pool_snapshot["ShortTokenOpenInterestLong"] = old_val / GMX_FLOAT_DECIMAL
                last_snapshot["ShortTokenOpenInterestLong"] = log_data["nextValue"] / GMX_FLOAT_DECIMAL
            else:
                pool_snapshot["ShortTokenOpenInterestShort"] = old_val / GMX_FLOAT_DECIMAL
                last_snapshot["ShortTokenOpenInterestShort"] = log_data["nextValue"] / GMX_FLOAT_DECIMAL
        else:
            raise RuntimeError("OpenInterestUpdated should have long or short token")


def _add_open_interest_in_tokens(pool_snapshot: Dict, pool_info: PoolInfo, tx_data, last_snapshot):
    logs = find_logs("OpenInterestInTokensUpdated", tx_data)
    for idx, log in logs.iterrows():
        log_data = ast.literal_eval(log["data"])
        old_val = log_data["nextValue"] - log_data["delta"]
        if pool_info.long_addr == log_data["collateralToken"]:
            if log_data["isLong"]:
                pool_snapshot["LongTokenOpenInterestInTokensLong"] = old_val / pool_info.index_decimal
                last_snapshot["LongTokenOpenInterestInTokensLong"] = log_data["nextValue"] / pool_info.index_decimal
            else:
                pool_snapshot["LongTokenOpenInterestInTokensShort"] = old_val / pool_info.index_decimal
                last_snapshot["LongTokenOpenInterestInTokensShort"] = log_data["nextValue"] / pool_info.index_decimal
        elif pool_info.short_addr == log_data["collateralToken"]:
            if log_data["isLong"]:
                pool_snapshot["ShortTokenOpenInterestInTokensLong"] = old_val / pool_info.index_decimal
                last_snapshot["ShortTokenOpenInterestInTokensLong"] = log_data["nextValue"] / pool_info.index_decimal
            else:
                pool_snapshot["ShortTokenOpenInterestInTokensShort"] = old_val / pool_info.index_decimal
                last_snapshot["ShortTokenOpenInterestInTokensShort"] = log_data["nextValue"] / pool_info.index_decimal
        else:
            raise RuntimeError("OpenInterestInTokensUpdated should have long or short token")


def _add_positon_fees(pool_snapshot: Dict, tx_data, last_snapshot: Dict):
    logs = find_logs("PositionFeesCollected", tx_data)
    for idx, log in logs.iterrows():
        log_data = ast.literal_eval(log["data"])
        pool_snapshot["borrowingFeeUsd"] += log_data["borrowingFeeUsd"] / GMX_FLOAT_DECIMAL


def _add_positon_decrease(pool_snapshot: Dict, tx_data):
    logs = find_logs("PositionDecrease", tx_data)
    for idx, log in logs.iterrows():
        log_data = ast.literal_eval(log["data"])
        pool_snapshot["realizedPnl"] += log_data["basePnlUsd"] / GMX_FLOAT_DECIMAL


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
        pool_info_simple = PoolInfo(  # to avoid too much 10**Decimal calculation
            10**pool_config.long_token.decimal,
            10**pool_config.short_token.decimal,
            10**pool_config.index_token.decimal,
            pool_config.long_token.address,
            pool_config.short_token.address,
            pool_config.index_token.address,
        )
        tick_df["market"] = tick_df["market"].str.lower()
        market_tick_df = tick_df[tick_df["market"] == pool_config.GM_address.lower()]
        market_tick_df = market_tick_df[(market_tick_df["tx_type"] != "") & (~market_tick_df["tx_type"].isna())]
        market_tick_df = market_tick_df.reset_index(drop=True)
        txes = market_tick_df.groupby(["block_number", "tx_index"])

        row_list = {}
        # for the last row of the day
        last_snapshot = {
            "timestamp": datetime.datetime.combine(day, datetime.time(23, 59, 59), tzinfo=datetime.timezone.utc)
        }
        # all take the first value(before this tx happend)
        with tqdm(total=len(txes), ncols=60, position=1, leave=False) as pbar:
            for (height, tx_index), tx_data in txes:
                # only values need initialization
                pool_snapshot = {
                    "timestamp": tx_data.iloc[0]["block_timestamp"],
                    "block_number": int(height),
                    "transaction_Hash": tx_data.iloc[0]["transaction_hash"],
                    "tx_type": tx_data.iloc[0]["tx_type"],
                    "longAmountDelta": 0,
                    "longAmountDeltaNoFee": 0,
                    "shortAmountDelta": 0,
                    "shortAmountDeltaNoFee": 0,
                    "borrowingFeeUsd": 0,
                    "realizedPnl": 0,
                }

                _add_pool_value_prop(pool_snapshot, pool_info_simple, tx_data)
                _add_pool_value_prop_last(pool_info_simple, tx_data, last_snapshot)
                _add_pool_amount_updated(pool_snapshot, pool_info_simple, tx_data, last_snapshot)
                _add_virtual_swap_inventory(pool_snapshot, pool_info_simple, tx_data, last_snapshot)
                _add_cumulative_borrowing_factor(pool_snapshot, pool_info_simple, tx_data, last_snapshot)
                _add_open_interest(pool_snapshot, pool_info_simple, tx_data, last_snapshot)
                _add_open_interest_in_tokens(pool_snapshot, pool_info_simple, tx_data, last_snapshot)
                _add_position_impact_pool_amount(pool_snapshot, pool_info_simple, tx_data, last_snapshot)
                _add_position_impact_pool_amount_v2(pool_snapshot, pool_info_simple, tx_data, last_snapshot)
                _add_swap_delta_in_deposits(pool_snapshot, pool_info_simple, tx_data)
                _add_swap_delta_in_swap(pool_snapshot, pool_info_simple, tx_data)
                _add_positon_fees(pool_snapshot, tx_data, last_snapshot)
                _add_positon_decrease(pool_snapshot, tx_data)

                # If funding fee amount per size, is not changed, it will show in position fees,
                # old value and new will be the same
                _add_unchanged_funding_fee_per_size(pool_snapshot, pool_info_simple, tx_data, last_snapshot)
                # if per size is changed, we will receive a FundingFeeAmountPerSizeUpdated, old value will be updated
                _add_funding_fee_amount_per_size(pool_snapshot, pool_info_simple, tx_data, last_snapshot)
                _add_claimable_funding_amount_per_size(pool_snapshot, pool_info_simple, tx_data, last_snapshot)

                row_list[pool_snapshot["block_number"]] = pool_snapshot
                pbar.update()

        virtual_market_df = tick_df[tick_df["market"] == pool_config.index_token.address]
        for idx, log_row in virtual_market_df.iterrows():
            if log_row["block_number"] in row_list:
                pool_snapshot = row_list[log_row["block_number"]]
                _add_virtual_position_inventory(pool_snapshot, pool_info_simple, log_row, last_snapshot)

        row_list[999999999999999999999] = last_snapshot
        df = pd.DataFrame(row_list.values())
        df.sort_values(["block_number"], inplace=True)

        for column_name in pool_file_columns:
            if column_name not in df.columns:
                df[column_name] = np.nan
        df = df[pool_file_columns]
        if pool_config.long_token.name.upper() == pool_config.short_token.name.upper():
            df[
                [
                    "LongTokenOpenInterestLong",
                    "LongTokenOpenInterestShort",
                    "ShortTokenOpenInterestLong",
                    "ShortTokenOpenInterestShort",
                    "LongTokenOpenInterestInTokensLong",
                    "LongTokenOpenInterestInTokensShort",
                    "ShortTokenOpenInterestInTokensLong",
                    "ShortTokenOpenInterestInTokensShort",
                ]
            ] /= 2
        return df
