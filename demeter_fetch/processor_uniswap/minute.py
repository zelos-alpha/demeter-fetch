import datetime
from dataclasses import dataclass
from typing import Dict, Callable

import pandas
import pandas as pd

import demeter_fetch.processor_uniswap.uniswap_utils as uniswap_utils
from demeter_fetch.common import DailyNode
from demeter_fetch.common import MinuteData, OnchainTxType, MinuteDataNames, UniNodesNames
from demeter_fetch.common import TextUtil, TimeUtil, DataUtil, to_decimal


class ModuleUtils(object):
    @staticmethod
    def get_datetime(date_str: str) -> datetime:
        if type(date_str) == pd.Timestamp:
            return date_str.to_pydatetime()
        else:
            return datetime.datetime.strptime(
                TextUtil.cut_after(str(date_str), "+").replace("T", " "), "%Y-%m-%d %H:%M:%S"
            )


@dataclass
class MinuteData:
    timestamp = 0
    netAmount0 = 0
    netAmount1 = 0
    closeTick = 0
    openTick = 0
    lowestTick = 0
    highestTick = 0
    inAmount0 = 0
    inAmount1 = 0
    currentLiquidity = 0


class UniMinute(DailyNode):
    def __init__(self, depends):
        super().__init__(depends)
        self.name = UniNodesNames.minute

    def get_file_name_by_day(self, day_str: str = "") -> str:
        return f"{self.from_config.chain.name}-{self.from_config.uniswap_config.pool_address}-{day_str}.minute.csv"

    def load_csv_converter(self) -> Dict[str, Callable]:
        return {
            "inAmount0": to_decimal,
            "inAmount1": to_decimal,
            "currentLiquidity": to_decimal,
            "netAmount0": to_decimal,
            "netAmount1": to_decimal,
        }

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: datetime.date):
        df = data[UniNodesNames.pool]

        df["block_timestamp"] = pd.to_datetime(df["block_timestamp"])
        df = df.set_index(keys=["block_timestamp"])
        df["tx_type"] = df.apply(lambda x: uniswap_utils.get_tx_type(x.topics), axis=1)
        df = df[df["tx_type"] == OnchainTxType.SWAP]
        decoded_df = pd.DataFrame()
        decoded_df[
            [
                "sender",
                "receipt",
                "amount0",
                "amount1",
                "sqrtPriceX96",
                "currentLiquidity",
                "current_tick",
                "tick_lower",
                "tick_upper",
                "liquidity",
                "delta_liquidity",
            ]
        ] = df.apply(lambda r: uniswap_utils.handle_event(r.tx_type, r.topics, r.data), axis=1, result_type="expand")
        decoded_df["inAmount0"] = decoded_df["amount0"].apply(lambda x: x if x > 0 else 0)
        decoded_df["inAmount1"] = decoded_df["amount1"].apply(lambda x: x if x > 0 else 0)
        minute_df = decoded_df.resample("1T").agg(
            {
                "amount0": "sum",
                "amount1": "sum",
                "inAmount0": "sum",
                "inAmount1": "sum",
                "currentLiquidity": "last",
            }
        )
        minute_df = minute_df.rename(columns={"amount0": "netAmount0", "amount1": "netAmount1"})
        minute_df[["openTick", "highestTick", "lowestTick", "closeTick"]] = (
            decoded_df["current_tick"]
            .resample("1T")
            .agg(
                {
                    "openTick": "first",
                    "highestTick": "max",
                    "lowestTick": "min",
                    "closeTick": "last",
                }
            )
        )
        minute_df["timestamp"] = minute_df.index

        minute_df = minute_df[
            [
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
        ]
        minute_df[["closeTick", "currentLiquidity"]] = minute_df[["closeTick", "currentLiquidity"]].ffill()
        minute_df[["netAmount0", "netAmount1", "inAmount0", "inAmount1"]] = minute_df[
            ["netAmount0", "netAmount1", "inAmount0", "inAmount1"]
        ].fillna(value=0)
        minute_df["openTick"] = minute_df["openTick"].fillna(minute_df["closeTick"])
        minute_df["highestTick"] = minute_df["highestTick"].fillna(minute_df["closeTick"])
        minute_df["lowestTick"] = minute_df["lowestTick"].fillna(minute_df["closeTick"])

        return minute_df


def preprocess_one(raw_data: pd.DataFrame) -> pd.DataFrame:
    if raw_data.size <= 0:
        return raw_data
    start_time = TimeUtil.get_minute(ModuleUtils.get_datetime(raw_data.loc[0, "block_timestamp"]))
    minute_rows = []
    data = []
    total_index = 1
    raw_data["tx_type"] = raw_data.apply(lambda x: uniswap_utils.get_tx_type(x.pool_topics), axis=1)

    for index, row in raw_data.iterrows():
        current_time = TimeUtil.get_minute(ModuleUtils.get_datetime(row["block_timestamp"]))
        if start_time == current_time:  # middle of a minute
            minute_rows.append(row)
        else:  #
            data.append(sample_data_to_one_minute(start_time, minute_rows))
            total_index += 1
            # start on_bar minute
            start_time = current_time
            minute_rows = [row]
    data = DataUtil.fill_missing(data)
    df = pandas.DataFrame(columns=MinuteDataNames, data=map(lambda d: d.to_array(), data))
    return df


def sample_data_to_one_minute(current_time, minute_rows) -> MinuteData:
    data = MinuteData()
    data.timestamp = current_time

    i = 1
    for r in minute_rows:
        (
            sender,
            receipt,
            amount0,
            amount1,
            sqrtPriceX96,
            current_liquidity,
            current_tick,
            tick_lower,
            tick_upper,
            liquidity,
            delta_liquidity,
        ) = uniswap_utils.handle_event(r.tx_type, r.pool_topics, r.pool_data)
        # print(tx_type, sender, receipt, amount0, amount1, sqrtPriceX96, current_liquidity, current_tick, tick_lower,
        #       tick_upper, delta_liquidity)
        match r.tx_type:
            case OnchainTxType.MINT:
                pass
            case OnchainTxType.BURN:
                pass
            case OnchainTxType.COLLECT:
                pass
            case OnchainTxType.SWAP:
                data.net_amount0 += amount0
                data.net_amount1 += amount1
                if amount0 > 0:
                    data.in_amount0 += amount0
                if amount1 > 0:
                    data.in_amount1 += amount1
                if data.openTick is None:  # first
                    data.open_tick = current_tick
                    data.highest_tick = current_tick
                    data.lowest_tick = current_tick
                if data.highest_tick < current_tick:
                    data.highest_tick = current_tick
                if data.lowest_tick > current_tick:
                    data.lowest_tick = current_tick
                if i == len(minute_rows):  # last
                    data.close_tick = current_tick
                    data.current_liquidity = current_liquidity

        i += 1
    return data
