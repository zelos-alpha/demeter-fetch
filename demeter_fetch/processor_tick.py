import asyncore
import datetime
import glob
import os
from decimal import Decimal
from typing import Dict, Set, Tuple

import pandas as pd

import demeter_fetch._typing as _typing
import demeter_fetch.uniswap_utils as uniswap_utils


def decode_file_name(file_path, file_name: str) -> Tuple[str, datetime.date]:
    temp_str = file_name.replace(file_path, "").replace(".csv", "").strip("/")
    date_str = temp_str[-10:]
    pool_address = temp_str[:-10]
    date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    return pool_address, date


def check_file(file_path, file_name, pool, start_date, end_date) -> bool:
    pool_address, date = decode_file_name(file_path, file_name)
    is_in = start_date < date < end_date
    return is_in and pool == pool_address


def merge_file(start_date, end_date, file_path, pool_address) -> pd.DataFrame:
    all_files = glob.glob(
        os.path.join(file_path, "*.csv")
    )  # advisable to use os.path.join as this makes concatenation OS independent

    wanted_files = [
        file
        for file in all_files
        if check_file(file_path, file, pool_address, start_date, end_date)
    ]

    df_from_each_file = (pd.read_csv(f) for f in wanted_files)
    concatenated_df = pd.concat(df_from_each_file, ignore_index=True)
    return concatenated_df


def process_duplicate_row(index, row, row_to_remove, df_count, df):
    if df_count.loc[row.key] > 1:
        row_to_remove.append(index)
    else:
        df.loc[index, "proxy_topics"] = ""
        pass


def data_is_not_empty(data):
    if isinstance(data, str) and data != "":
        return True
    return False


def compare_int_with_error(a, b, error=1):
    if abs(int(a, 16) - int(b, 16)) > error:
        return False
    return True


def is_collect_data_same(a: str, b: str) -> bool:
    if a == b:
        return True
    if not compare_int_with_error(a[66:130], b[66:130]):
        return False
    if not compare_int_with_error(a[130:], b[130:]):
        return False
    return True


def is_burn_data_same(a: str, b: str) -> bool:
    if a == b:
        return True
    if not compare_int_with_error(a[0:66], b[0:66]):
        return False
    if not compare_int_with_error(a[66:130], b[66:130]):
        return False
    return compare_int_with_error(a[130:], b[130:])


def drop_duplicate(df: pd.Series):
    """
    由于sql用pool表join proxy表. 虽然用一个tx_hash做join条件, 但是当一个tx_hash中有多个mint或者burn的时候, 会产生冗余记录.
    因此用对比log.data的方法, 删除重复记录

    重复情况有
    1: 两个pool.topic. 带有两个proxy.topic, 此时会产生4条记录. 对比data删掉不匹配的2个
    2: 两个pool.topic, 但是只有一个proxy.topic, 此时不会有多余记录, 但是其中一个proxy.topic是不匹配的. 需要找到不匹配的那个, 然后删除topic字段.
    """
    row_to_remove = []
    df_count = df["key"].value_counts()
    for index, row in df.iterrows():
        if row.tx_type == _typing.OnchainTxType.SWAP:
            pass
        elif row.tx_type == _typing.OnchainTxType.MINT:
            if (
                data_is_not_empty(row.proxy_data)
                and row.pool_data[66:] != row.proxy_data[2:]
            ):
                process_duplicate_row(index, row, row_to_remove, df_count, df)
        elif row.tx_type == _typing.OnchainTxType.COLLECT:
            if data_is_not_empty(row.proxy_data) and not is_collect_data_same(
                row.pool_data, row.proxy_data
            ):
                process_duplicate_row(index, row, row_to_remove, df_count, df)
        elif row.tx_type == _typing.OnchainTxType.BURN:
            if data_is_not_empty(row.proxy_data) and not is_burn_data_same(
                row.pool_data, row.proxy_data
            ):
                process_duplicate_row(index, row, row_to_remove, df_count, df)
        else:
            raise ValueError("not support tx type")
    df.drop(index=row_to_remove, inplace=True)

    # 用另一种方式重新处理一次, 用于处理同一个交易中data相同的情况
    # https://polygonscan.com/tx/0x2f7577ddd83165bdf18af41dc7d712c19523baa3f7fc0244cf58b7003157828c#eventlog
    # 处理方式: 对于pool log, 保留第一个匹配, 且没有被使用过的peoxy log, 剩下的删掉
    row_to_remove = []
    df_count: pd.Series = df["key"].value_counts()
    df_count: pd.Series = df_count[df_count > 1]

    taken_proxy_log: Dict[str, Set[int]] = {}
    for key, value in df_count.items():
        duplicate_txes = df[df["key"] == key]
        taken = False

        for dup_index, dup_value in duplicate_txes.iterrows():
            # init taken_proxy_log
            if dup_value["transaction_hash"] not in taken_proxy_log:
                taken_proxy_log[dup_value["transaction_hash"]] = set()
                taken = False
            if (
                not taken
                and dup_value["proxy_log_index"]
                not in taken_proxy_log[dup_value["transaction_hash"]]
            ):
                taken_proxy_log[dup_value["transaction_hash"]].add(
                    dup_value["proxy_log_index"]
                )
                taken = True
            else:
                row_to_remove.append(dup_index)

    df.drop(index=row_to_remove, inplace=True)


def handle_tick(lower_tick, upper_tick, current_tick, delta):
    if lower_tick is None or upper_tick is None or current_tick is None:
        return 0
    if lower_tick < current_tick < upper_tick:
        return delta
    else:
        return 0


def preprocess(pool_address, start_date, end_date, data_file_path):
    # FIXME BUG: start == end。  merge fail
    df = merge_file(start_date, end_date, data_file_path, pool_address)

    return preprocess_one(df)


def convert_to_decimal(value):
    return Decimal(value) if value else Decimal(0)


def preprocess_one(df):
    df["tx_type"] = df.apply(lambda x: uniswap_utils.get_tx_type(x.pool_topics), axis=1)
    df["key"] = df.apply(
        lambda x: x.transaction_hash + "_" + str(x.pool_log_index), axis=1
    )
    drop_duplicate(df)
    # FIXME BUG: start == end。  merge fail
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
    ] = df.apply(
        lambda x: uniswap_utils.handle_event(x.tx_type, x.pool_topics, x.pool_data),
        axis=1,
        result_type="expand",
    )
    # block_number, block_timestamp, tx_type, transaction_hash, pool_tx_index, pool_log_index, proxy_log_index, sender, receipt, amount0, amount1, total_liquidity, total_liquidity_delta, sqrtPriceX96, current_tick, position_id, tick_lower, tick_upper, liquidity
    df["position_id"] = df.apply(
        lambda x: uniswap_utils.handle_proxy_event(x.proxy_topics), axis=1
    )
    df = df.drop(
        columns=["pool_topics", "pool_data", "proxy_topics", "key", "proxy_data"]
    )
    df = df.sort_values(["block_number", "pool_log_index"], ascending=[True, True])
    df[["sqrtPriceX96", "total_liquidity", "current_tick"]] = df[
        ["sqrtPriceX96", "total_liquidity", "current_tick"]
    ].fillna(method="ffill")

    df["total_liquidity_delta"] = df["total_liquidity_delta"].fillna(0)
    # convert type to keep decimal
    df["sqrtPriceX96"] = df.apply(lambda x: convert_to_decimal(x.sqrtPriceX96), axis=1)
    df["total_liquidity_delta"] = df.apply(
        lambda x: convert_to_decimal(x.total_liquidity_delta), axis=1
    )
    df["liquidity"] = df.apply(lambda x: convert_to_decimal(x.liquidity), axis=1)
    df["total_liquidity"] = df.apply(
        lambda x: convert_to_decimal(x.total_liquidity), axis=1
    )

    df["total_liquidity_delta"] = df.apply(
        lambda x: handle_tick(
            x.tick_lower, x.tick_upper, x.current_tick, x.total_liquidity_delta
        ),
        axis=1,
    )
    df["total_liquidity"] = df.apply(
        lambda x: x.total_liquidity_delta + x.total_liquidity, axis=1
    )
    df["block_timestamp"] = df["block_timestamp"].apply(lambda x: x.split("+")[0])
    df["tx_type"] = df.apply(lambda x: x.tx_type.name, axis=1)
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
    df = df[order]
    return df
