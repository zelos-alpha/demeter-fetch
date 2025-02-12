#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-10 11:08
# @Author  : 32ethers
# @Description:
from datetime import date
from typing import List

import pandas as pd

from .big_query_utils import BigQueryChain, query_by_sql
from .. import ChainTypeConfig
from ..common import FromConfig, utils, KECCAK


def _update_df(df: pd.DataFrame) -> pd.DataFrame:
    if len(df.index) < 1:
        return df
    df["topics"] = df["topics"].apply(lambda x: x.tolist())
    if len(df.index) < 1:
        df = pd.DataFrame(
            columns=[
                "block_number",
                "block_timestamp",
                "transaction_hash",
                "transaction_index",
                "log_index",
                "topics",
                "data",
            ]
        )
    else:
        df = df.sort_values(["block_number", "log_index"], ascending=[True, True])
        df["block_timestamp"] = df["block_timestamp"].dt.tz_localize(None)
        # pd.Series(df["block_timestamp"].dt.to_pydatetime(), index=df.index, dtype=object)
        df["block_timestamp"] = pd.to_datetime(df["block_timestamp"])
    return df


def bigquery_pool(config: FromConfig, day: date):
    day_str = day.strftime("%Y-%m-%d")
    sql = f"""
    SELECT block_number,block_timestamp, transaction_hash , transaction_index , log_index, topics , DATA as data
        FROM {BigQueryChain[config.chain.name].value["table_name"]}
        WHERE  topics[SAFE_OFFSET(0)] in {KECCAK.SWAP.value, KECCAK.BURN.value, KECCAK.COLLECT.value, KECCAK.MINT.value}
            AND DATE(block_timestamp) =  DATE("{day_str}") AND address = "{config.uniswap_config.pool_address}"
    """
    df = query_by_sql(sql, config.big_query.auth_file, config.http_proxy)
    df = _update_df(df)
    return df


def bigquery_v4_pool(config: FromConfig, day: date):
    day_str = day.strftime("%Y-%m-%d")
    sql = f"""
    SELECT block_number,block_timestamp, transaction_hash , transaction_index , log_index, topics , DATA as data
        FROM {BigQueryChain[config.chain.name].value["table_name"]}
        WHERE topics[SAFE_OFFSET(0)] in {KECCAK.UNI_V4_SWAP.value, KECCAK.UNI_V4_MODIFY_LIQ.value}
        AND topics[SAFE_OFFSET(1)] = "{config.uniswap_config.pool_address}"
        AND DATE(block_timestamp) =  DATE("{day_str}") AND address = "{ChainTypeConfig[config.chain]["uni_v4_pool_manager"]}"
    """
    df = query_by_sql(sql, config.big_query.auth_file, config.http_proxy)
    df = _update_df(df)
    return df


def bigquery_proxy_lp(config: FromConfig, day: date):
    day_str = day.strftime("%Y-%m-%d")
    sql = f"""
    SELECT block_number,block_timestamp, transaction_hash , transaction_index , log_index, topics , DATA as data
        FROM {BigQueryChain[config.chain.name].value["table_name"]}
        WHERE  topics[SAFE_OFFSET(0)] in {KECCAK.UNI_PROXY_INCREASE.value, KECCAK.UNI_PROXY_DECREASE.value, KECCAK.UNI_PROXY_COLLECT.value}
            AND DATE(block_timestamp) =  DATE("{day_str}") AND address = "{ChainTypeConfig[config.chain]["uniswap_proxy_addr"]}"
    """
    df = query_by_sql(sql, config.big_query.auth_file, config.http_proxy)
    df = _update_df(df)
    return df


def bigquery_proxy_transfer(config: FromConfig, day: date):
    day_str = day.strftime("%Y-%m-%d")
    sql = f"""
    SELECT block_number,block_timestamp, transaction_hash , transaction_index , log_index, topics , DATA as data
        FROM {BigQueryChain[config.chain.name].value["table_name"]}
        WHERE  topics[SAFE_OFFSET(0)] in ('{KECCAK.TRANSFER.value}')
            AND DATE(block_timestamp) =  DATE("{day_str}") AND address = "{ChainTypeConfig[config.chain]["uniswap_proxy_addr"]}"
    """
    df = query_by_sql(sql, config.big_query.auth_file, config.http_proxy)
    df["topics"] = df["topics"].apply(lambda x: x.tolist())
    return df


def bigquery_transaction(config: FromConfig, day: date, tx: List[str]):
    day_str = day.strftime("%Y-%m-%d")
    tx_str = ",".join(['"' + utils.hex_to_length(x, 64) + '"' for x in tx])
    sql = f"""
    select `hash` as transaction_hash ,block_number,transaction_index,from_address as `from`,to_address as `to`,value
    from {BigQueryChain[config.chain.name].value["tx_table_name"]}
    where DATE(block_timestamp) = DATE("{day_str}")
        and  `hash` in ({tx_str})
    """
    df = query_by_sql(sql, config.big_query.auth_file, config.http_proxy)
    df["value"] = df["value"].apply(lambda x: int(x))
    return df


def bigquery_aave(config: FromConfig, day: date, tokens: List[str]):
    day_str = day.strftime("%Y-%m-%d")
    token_str = ",".join(['"' + utils.hex_to_length(x, 64) + '"' for x in tokens])
    keccak_str = ",".join(
        [
            '"' + utils.hex_to_length(x, 64) + '"'
            for x in [
                KECCAK.AAVE_SUPPLY.value,
                KECCAK.AAVE_REPAY.value,
                KECCAK.AAVE_BORROW.value,
                KECCAK.AAVE_WITHDRAW.value,
                KECCAK.AAVE_LIQUIDATION.value,
                KECCAK.AAVE_UPDATED.value,
            ]
        ]
    )
    sql = f"""
            SELECT block_number,block_timestamp,transaction_hash,transaction_index,log_index,topics,DATA as data
            FROM {BigQueryChain[config.chain.name].value["table_name"]}
            WHERE
              topics[SAFE_OFFSET(0)] IN ({keccak_str})
              AND topics[SAFE_OFFSET(1)] IN ({token_str})
              AND DATE(block_timestamp) >= DATE("{day_str}")
              AND DATE(block_timestamp) <= DATE("{day_str}")
              AND address = "{ChainTypeConfig[config.chain]['aave_v3_pool_addr']}"
        """
    df = query_by_sql(sql, config.big_query.auth_file, config.http_proxy)
    df = _update_df(df)
    return df
