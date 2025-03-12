import datetime
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Dict, Callable, List

import pandas as pd

from .uniswap_utils import match_proxy_log, handle_event, handle_proxy_event, handle_v4_event
from ..common import to_decimal, DailyNode, NodeNames, DailyParam, get_tx_type, get_depend_name, FriendFuncName

tick_file_columns = [
    "block_number",
    "block_timestamp",
    "tx_type",
    "transaction_hash",
    "tx_index",
    "log_index",
    "sender",
    "receipt",
    "amount0",
    "amount1",
    "total_liquidity",
    "total_liquidity_delta",
    "sqrtPriceX96",
    "current_tick",
    "tick_lower",
    "tick_upper",
    "liquidity",
]

v4_tick_file_columns = [
    "block_number",
    "block_timestamp",
    "tx_type",
    "transaction_hash",
    "tx_index",
    "log_index",
    "sender",
    "amount0",
    "amount1",
    "current_tick",
    "sqrtPriceX96",
    "fee",
    "total_liquidity",
    "position_id",
    "tick_lower",
    "tick_upper",
    "liquidity",
    "total_liquidity_delta",
]


@dataclass
class PoolTick:
    block_number = 0
    block_timestamp = 0
    tx_type = 0
    transaction_hash = 0
    pool_tx_index = 0
    pool_log_index = 0
    proxy_log_index = 0
    sender = 0
    receipt = 0
    amount0 = 0
    amount1 = 0
    total_liquidity = 0
    total_liquidity_delta = 0
    sqrtPriceX96 = 0
    current_tick = 0
    position_id = 0
    tick_lower = 0
    tick_upper = 0
    liquidity = 0


def convert_pool_tick_df(input_df: pd.DataFrame) -> pd.DataFrame:
    if len(input_df.index) < 1:
        return pd.DataFrame(columns=tick_file_columns)

    df = input_df.copy()
    df["tx_type"] = df.apply(lambda x: get_tx_type(x.topics), axis=1)

    df[
        [
            "sender",
            "receipt",
            "amount0",
            "amount1",
            "sqrtPriceX96",
            "total_liquidity",
            "current_tick",
            "tick_lower",
            "tick_upper",
            "liquidity",
            "total_liquidity_delta",
        ]
    ] = df.apply(lambda r: handle_event(r.tx_type, r.topics, r.data), axis=1, result_type="expand")
    df = df.drop(columns=["topics", "data"])
    df = df.sort_values(["block_number", "log_index"], ascending=[True, True])
    df[["sqrtPriceX96", "total_liquidity", "current_tick"]] = df[
        ["sqrtPriceX96", "total_liquidity", "current_tick"]
    ].ffill()
    with pd.option_context("future.no_silent_downcasting", True):
        df["total_liquidity_delta"] = df["total_liquidity_delta"].fillna(0).infer_objects(copy=False)
    # convert type to keep decimal
    df["sqrtPriceX96"] = df.apply(lambda x: convert_to_decimal(x.sqrtPriceX96), axis=1)
    df["total_liquidity_delta"] = df.apply(lambda x: convert_to_decimal(x.total_liquidity_delta), axis=1)
    df["liquidity"] = df.apply(lambda x: convert_to_decimal(x.liquidity), axis=1)
    df["total_liquidity"] = df.apply(lambda x: convert_to_decimal(x.total_liquidity), axis=1)

    df["total_liquidity_delta"] = df.apply(
        lambda x: get_active_liquidity(x.tick_lower, x.tick_upper, x.current_tick, x.total_liquidity_delta),
        axis=1,
    )
    df["total_liquidity"] = df.apply(lambda x: x.total_liquidity_delta + x.total_liquidity, axis=1)
    df["tx_type"] = df.apply(lambda x: x.tx_type.name, axis=1)
    df = df.rename(columns={"transaction_index": "tx_index"})

    df = df[tick_file_columns]

    return df


class UniTick(DailyNode):
    name = NodeNames.uni_tick

    def _get_file_name(self, param: DailyParam) -> str:
        return (
            f"{self.from_config.chain.name}-{self.from_config.uniswap_config.pool_address}-{param.day.strftime('%Y-%m-%d')}.tick"
            + self._get_file_ext()
        )

    @property
    def _load_csv_converter(self) -> Dict[str, Callable]:
        return {
            "amount0": to_decimal,
            "amount1": to_decimal,
            "liquidity": to_decimal,
            "total_liquidity": to_decimal,
            "total_liquidity_delta": to_decimal,
            "sqrtPriceX96": to_decimal,
        }

    @property
    def _parse_date_column(self) -> List[str]:
        return ["block_timestamp"]

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: datetime.date) -> pd.DataFrame:
        pool_df = data[get_depend_name(NodeNames.uni_pool, self.id)]
        proxy_df = data[get_depend_name(NodeNames.uni_proxy_lp, self.id)]
        match_proxy_log(pool_df, proxy_df)
        pool_df = pool_df.sort_values(["block_number", "log_index"], ascending=[True, True])

        merged_df = convert_pool_tick_df(pool_df)
        merged_df[["proxy_topics", "proxy_data", "proxy_log_index"]] = pool_df[
            ["proxy_topics", "proxy_data", "proxy_log_index"]
        ]
        merged_df.rename(columns={"tx_index": "pool_tx_index", "log_index": "pool_log_index"}, inplace=True)

        merged_df["position_id"] = merged_df.apply(lambda x: handle_proxy_event(x.proxy_topics), axis=1)
        order = [
            "block_number",
            "block_timestamp",
            "tx_type",
            "transaction_hash",
            "pool_tx_index",
            "pool_log_index",
            "proxy_log_index",
            "sender",
            "receipt",
            "amount0",
            "amount1",
            "total_liquidity",
            "total_liquidity_delta",
            "sqrtPriceX96",
            "current_tick",
            "position_id",
            "tick_lower",
            "tick_upper",
            "liquidity",
        ]
        merged_df = merged_df[order]
        return merged_df


class UniTickNoPos(DailyNode):
    name = NodeNames.uni_tick_without_pos

    def _get_file_name(self, param: DailyParam) -> str:
        return (
            f"{self.from_config.chain.name}-{self.from_config.uniswap_config.pool_address}-{param.day.strftime('%Y-%m-%d')}.pool.tick"
            + self._get_file_ext()
        )

    @property
    def _load_csv_converter(self) -> Dict[str, Callable]:
        return {
            "amount0": to_decimal,
            "amount1": to_decimal,
            "total_liquidity": to_decimal,
            "total_liquidity_delta": to_decimal,
            "sqrtPriceX96": to_decimal,
        }

    @property
    def _parse_date_column(self) -> List[str]:
        return ["block_timestamp"]

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: datetime.date) -> pd.DataFrame:
        input_param = data[get_depend_name(NodeNames.uni_pool, self.id)]
        df = convert_pool_tick_df(input_param)
        return df


def convert_to_decimal(value):
    return Decimal(value) if value else Decimal(0)


def get_active_liquidity(lower_tick, upper_tick, current_tick, delta):
    if lower_tick is None or upper_tick is None or current_tick is None:
        return 0
    if lower_tick < current_tick < upper_tick:
        return delta
    else:
        return 0


class UniV4Tick(UniTick):
    name = NodeNames.uni4_tick

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: date) -> pd.DataFrame:
        input_df = data[get_depend_name(NodeNames.uni4_pool, self.id)]
        if len(input_df.index) < 1:
            return pd.DataFrame(columns=tick_file_columns)
        df = input_df.copy()
        df["tx_type"] = df.apply(lambda x: get_tx_type(x.topics), axis=1)

        df[
            [
                "pool_id",
                "sender",
                "amount0",
                "amount1",
                "sqrtPriceX96",
                "total_liquidity",
                "current_tick",
                "tick_lower",
                "tick_upper",
                "liquidity",
                "fee",
                "position_id",
            ]
        ] = df.apply(lambda r: handle_v4_event(r.tx_type, r.topics, r.data), axis=1, result_type="expand")
        df = df.drop(columns=["topics", "data"])
        df = df.sort_values(["block_number", "log_index"], ascending=[True, True])
        df[["sqrtPriceX96", "total_liquidity", "current_tick"]] = df[
            ["sqrtPriceX96", "total_liquidity", "current_tick"]
        ].ffill()

        # convert type to keep decimal
        df["sqrtPriceX96"] = df.apply(lambda x: convert_to_decimal(x.sqrtPriceX96), axis=1)
        df["liquidity"] = df.apply(lambda x: convert_to_decimal(x.liquidity), axis=1)
        df["total_liquidity"] = df.apply(lambda x: convert_to_decimal(x.total_liquidity), axis=1)

        df["total_liquidity_delta"] = df.apply(
            lambda x: get_active_liquidity(x.tick_lower, x.tick_upper, x.current_tick, x.liquidity),
            axis=1,
        )
        df["total_liquidity"] = df.apply(lambda x: x.total_liquidity_delta + x.total_liquidity, axis=1)
        df["tx_type"] = df.apply(lambda x: FriendFuncName[x.tx_type], axis=1)
        df = df.rename(columns={"transaction_index": "tx_index"})

        df = df[v4_tick_file_columns]

        return df
