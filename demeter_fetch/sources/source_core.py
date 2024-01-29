#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-10 10:44
# @Author  : 32ethers
# @Description:
from dataclasses import dataclass
from datetime import date
from typing import Dict, List

import pandas as pd

from .big_query import bigquery_aave, bigquery_pool, bigquery_proxy_lp, bigquery_proxy_transfer
from .chifra import chifra_pool, chifra_proxy_lp, chifra_proxy_transfer
from .rpc import rpc_pool, rpc_proxy_lp, rpc_proxy_transfer, rpc_uni_tx
from ..common import DataSource, UniNodesNames, AaveNodesNames, DailyNode, Node, DailyParam, AaveDailyNode, utils
from ..common.nodes import AaveDailyParam


@dataclass
class EventLog:
    block_number = 0
    block_timestamp = 0
    transaction_hash = 0
    transaction_index = 0
    log_index = 0
    topics = 0
    data = 0


class UniSourcePool(DailyNode):
    def __init__(self, depends):
        super().__init__(depends)
        self.name = UniNodesNames.pool

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: date):
        df: pd.DataFrame | None = None
        match self.from_config.data_source:
            case DataSource.big_query:
                df = bigquery_pool(self.from_config, day)
            case DataSource.rpc:
                df = rpc_pool(self.from_config, self.to_path, day)
            case DataSource.chifra:
                df = chifra_pool(self.from_config, self.to_path, day)
        return df

    def _get_file_name(self, param: DailyParam) -> str:
        return f"{self.from_config.chain.name}-{self.from_config.uniswap_config.pool_address}-{param.day.strftime('%Y-%m-%d')}.raw.csv"


class UniSourceProxyLp(DailyNode):
    def __init__(self, depends):
        super().__init__(depends)
        self.name = UniNodesNames.proxy_lp

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: date):
        df: pd.DataFrame | None = None
        match self.from_config.data_source:
            case DataSource.big_query:
                df = bigquery_proxy_lp(self.from_config, day)
            case DataSource.rpc:
                df = rpc_proxy_lp(self.from_config, self.to_path, day)
            case DataSource.chifra:
                df = chifra_proxy_lp(self.from_config, self.to_path, day)
        return df

    def _get_file_name(self, param: DailyParam) -> str:
        return f"{self.from_config.chain.name}-uniswap-proxy-lp-{param.day.strftime('%Y-%m-%d')}.raw.csv"


class UniSourceProxyTransfer(DailyNode):
    def __init__(self, depends):
        super().__init__(depends)
        self.name = UniNodesNames.proxy_transfer

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: date):
        df: pd.DataFrame | None = None
        match self.from_config.data_source:
            case DataSource.big_query:
                return bigquery_proxy_transfer(self.from_config, day)
            case DataSource.rpc:
                df = rpc_proxy_transfer(self.from_config, self.to_path, day)
            case DataSource.chifra:
                df = chifra_proxy_transfer(self.from_config, self.to_path, day)
        return df

    def _get_file_name(self, param: DailyParam) -> str:
        return f"{self.from_config.chain.name}-uniswap-proxy-transfer-{param.day.strftime('%Y-%m-%d')}.raw.csv"


class AaveSource(AaveDailyNode):
    def __init__(self, depends):
        super().__init__(depends)
        self.name = AaveNodesNames.raw

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: date, tokens: List[str]) -> Dict[str, pd.DataFrame]:
        df: pd.DataFrame | None = None
        match self.from_config.data_source:
            case DataSource.big_query:
                df = bigquery_aave(self.from_config, day, tokens)
            case DataSource.rpc:
                raise NotImplementedError()
            case DataSource.chifra:
                raise NotImplementedError()
        df["token"] = df["topics"].apply(lambda x: utils.hex_to_length(x[1], 40))
        tokens_df = {}
        for token_addr, token_df in df.groupby(["token"]):
            clean_df = token_df.drop(columns=["token"])
            tokens_df[token_addr[0]] = clean_df
        return tokens_df

    def _get_file_name(self, param: AaveDailyParam) -> str:
        return f"{self.from_config.chain.name}-aave_v3-{param.token}-{param.day.strftime('%Y-%m-%d')}.raw.csv"


class UniTransaction(DailyNode):
    def __init__(self, depends):
        super().__init__(depends)
        self.name = UniNodesNames.tx

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: date):
        tick_df = data[UniNodesNames.tick]
        tick_df = tick_df[tick_df["tx_type"].isin(["MINT", "BURN", "COLLECT"])]
        tx = tick_df["transaction_hash"].drop_duplicates()
        df: pd.DataFrame | None = None
        match self.from_config.data_source:
            case DataSource.big_query:
                raise NotImplementedError()
            case DataSource.rpc:
                df = rpc_uni_tx(self.from_config, tx)
            case DataSource.chifra:
                raise NotImplementedError()
        return df

    def _get_file_name(self, param: DailyParam) -> str:
        return f"{self.from_config.chain.name}-uniswap-pool-tx-{param.day.strftime('%Y-%m-%d')}.raw.csv"
