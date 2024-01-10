#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-10 11:08
# @Author  : 32ethers
# @Description:
from datetime import date

import pandas as pd

from ..common import FromConfig, Config, constants
from .big_query_utils import BigQueryChain, query_by_sql


def _update_df(df: pd.DataFrame) -> pd.DataFrame:
    df["topics"] = df["topics"].apply(lambda x: x.tolist())
    df = df.sort_values(["block_number", "log_index"], ascending=[True, True])
    df["block_timestamp"] = df["block_timestamp"].dt.tz_localize(None)
    df["block_timestamp"] = pd.Series(df["block_timestamp"].dt.to_pydatetime(), index=df.index, dtype=object)
    return df


def bigquery_pool(config: FromConfig, day: date):
    day_str = day.strftime("%Y-%m-%d")
    sql = f"""
    SELECT block_number,block_timestamp, transaction_hash , transaction_index , log_index, topics , DATA as data
        FROM {BigQueryChain[config.chain.value].value["table_name"]}
        WHERE  topics[SAFE_OFFSET(0)] in {constants.SWAP_KECCAK, constants.BURN_KECCAK, constants.COLLECT_KECCAK, constants.MINT_KECCAK}
            AND DATE(block_timestamp) =  DATE("{day_str}") AND address = "{config.uniswap_config.pool_address}"
    """
    df = query_by_sql(sql, config.big_query.auth_file, config.http_proxy)
    df = _update_df(df)
    return df


def bigquery_proxy_lp(config: FromConfig, day: date):
    day_str = day.strftime("%Y-%m-%d")
    sql = f"""
    SELECT block_number,block_timestamp, transaction_hash , transaction_index , log_index, topics , DATA as data
        FROM {BigQueryChain[config.chain.value].value["table_name"]}
        WHERE  topics[SAFE_OFFSET(0)] in {constants.INCREASE_LIQUIDITY, constants.DECREASE_LIQUIDITY, constants.COLLECT}
            AND DATE(block_timestamp) =  DATE("{day_str}") AND address = "{BigQueryChain[config.chain.value].value["uni_proxy_addr"]}"
    """
    df = query_by_sql(sql, config.big_query.auth_file, config.http_proxy)
    df["topics"] = df["topics"].apply(lambda x: x.tolist())
    return df


def bigquery_proxy_transfer(config: FromConfig, day: date):
    day_str = day.strftime("%Y-%m-%d")
    sql = f"""
    SELECT block_number,block_timestamp, transaction_hash , transaction_index , log_index, topics , DATA as data
        FROM {BigQueryChain[config.chain.value].value["table_name"]}
        WHERE  topics[SAFE_OFFSET(0)] in ('{constants.TRANSFER_KECCAK}')
            AND DATE(block_timestamp) =  DATE("{day_str}") AND address = "{BigQueryChain[config.chain.value].value["uni_proxy_addr"]}"
    """
    df = query_by_sql(sql, config.big_query.auth_file, config.http_proxy)
    df["topics"] = df["topics"].apply(lambda x: x.tolist())
    return df


def bigquery_aave(config: Config, day: date):
    return "bigquery_aave"
