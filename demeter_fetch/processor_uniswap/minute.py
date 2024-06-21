import datetime
from dataclasses import dataclass
from typing import Dict, Callable, List

import pandas as pd

import demeter_fetch.processor_uniswap.uniswap_utils as uniswap_utils
from demeter_fetch.common import DailyNode, DailyParam, get_tx_type, get_depend_name
from demeter_fetch.common import KECCAK, NodeNames
from demeter_fetch.common import TextUtil, to_decimal

columns = [
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


class ModuleUtils(object):
    @staticmethod
    def get_datetime(date_str: str) -> datetime:
        if isinstance(date_str, pd.Timestamp):
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
    name = NodeNames.uni_minute

    def _get_file_name(self, param: DailyParam) -> str:
        return (
            f"{self.from_config.chain.name}-{self.from_config.uniswap_config.pool_address}-{param.day.strftime('%Y-%m-%d')}.minute"
            + self._get_file_ext()
        )

    @property
    def _load_csv_converter(self) -> Dict[str, Callable]:
        return {
            "inAmount0": to_decimal,
            "inAmount1": to_decimal,
            "currentLiquidity": to_decimal,
            "netAmount0": to_decimal,
            "netAmount1": to_decimal,
        }

    @property
    def _parse_date_column(self) -> List[str]:
        return ["timestamp"]

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: datetime.date) -> pd.DataFrame:
        df = data[get_depend_name(NodeNames.uni_pool, self.id)]
        if len(df.index) < 1:
            return pd.DataFrame(columns=columns)
        df["block_timestamp"] = pd.to_datetime(df["block_timestamp"])
        df = df.set_index(keys=["block_timestamp"])
        df["tx_type"] = df.apply(lambda x: get_tx_type(x.topics), axis=1)
        df = df[df["tx_type"] == KECCAK.SWAP]
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
        minute_df = decoded_df.resample("1min").agg(
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
            .resample("1min")
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

        minute_df = minute_df[columns]
        minute_df[["closeTick", "currentLiquidity"]] = minute_df[["closeTick", "currentLiquidity"]].ffill()
        minute_df[["netAmount0", "netAmount1", "inAmount0", "inAmount1"]] = minute_df[
            ["netAmount0", "netAmount1", "inAmount0", "inAmount1"]
        ].fillna(value=0)
        minute_df["openTick"] = minute_df["openTick"].fillna(minute_df["closeTick"])
        minute_df["highestTick"] = minute_df["highestTick"].fillna(minute_df["closeTick"])
        minute_df["lowestTick"] = minute_df["lowestTick"].fillna(minute_df["closeTick"])

        return minute_df
