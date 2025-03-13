#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-10 10:44
# @Author  : 32ethers
# @Description:
from dataclasses import dataclass
from datetime import date
from typing import Dict, List

import pandas as pd

from .big_query import (
    bigquery_aave,
    bigquery_v4_pool,
    bigquery_pool,
    bigquery_proxy_lp,
    bigquery_proxy_transfer,
    bigquery_transaction,
)
from .chifra import chifra_pool, chifra_proxy_lp, chifra_proxy_transfer, chifra_aave
from .rpc import (
    rpc_pool,
    rpc_proxy_lp,
    rpc_proxy_transfer,
    rpc_uni_tx,
    rpc_aave,
    rpc_squeeth,
    rpc_uni_v4_pool,
    rpc_gmx_v2,
)
from .. import ToFileType
from ..common import DataSource, NodeNames, DailyNode, DailyParam, AaveDailyNode, utils, get_depend_name
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
    name = NodeNames.uni_pool

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
        return (
            f"{self.from_config.chain.name}-{self.from_config.uniswap_config.pool_address}-{param.day.strftime('%Y-%m-%d')}.raw"
            + self._get_file_ext()
        )

    @property
    def _parse_date_column(self) -> List[str]:
        return ["block_timestamp"]


class UniV4SourcePool(DailyNode):
    name = NodeNames.uni4_pool

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: date):
        df: pd.DataFrame | None = None
        match self.from_config.data_source:
            case DataSource.rpc:
                df = rpc_uni_v4_pool(self.from_config, self.to_path, day)
            case DataSource.big_query:
                df = bigquery_v4_pool(self.from_config, day)
        return df

    def _get_file_name(self, param: DailyParam) -> str:
        return (
            f"{self.from_config.chain.name}-{self.from_config.uniswap_config.pool_address}-{param.day.strftime('%Y-%m-%d')}.raw"
            + self._get_file_ext()
        )

    @property
    def _parse_date_column(self) -> List[str]:
        return ["block_timestamp"]


class UniSourceProxyLp(DailyNode):
    name = NodeNames.uni_proxy_lp

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
        return (
            f"{self.from_config.chain.name}-uniswap-proxy-lp-{param.day.strftime('%Y-%m-%d')}.raw"
            + self._get_file_ext()
        )


class UniSourceProxyTransfer(DailyNode):
    name = NodeNames.uni_proxy_transfer

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
        return (
            f"{self.from_config.chain.name}-uniswap-proxy-transfer-{param.day.strftime('%Y-%m-%d')}.raw"
            + self._get_file_ext()
        )

    @property
    def _parse_date_column(self) -> List[str]:
        return ["block_timestamp"]


class UniTransaction(DailyNode):
    name = NodeNames.uni_tx

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: date):
        tick_df = data[get_depend_name(NodeNames.uni_tick, self.id)]
        tick_df = tick_df[tick_df["tx_type"].isin(["MINT", "BURN", "COLLECT"])]
        tx = tick_df["transaction_hash"].drop_duplicates()
        df: pd.DataFrame | None = None
        match self.from_config.data_source:
            case DataSource.big_query:
                df = bigquery_transaction(self.from_config, day, tx)
            case DataSource.rpc:
                df = rpc_uni_tx(self.from_config, tx)
            case DataSource.chifra:
                raise NotImplementedError()
        df.sort_values(by=["block_number", "transaction_index"], ascending=True, inplace=True)
        return df

    def _get_file_name(self, param: DailyParam) -> str:
        return (
            f"{self.from_config.chain.name}-{self.from_config.uniswap_config.pool_address}-{param.day.strftime('%Y-%m-%d')}.tx.raw"
            + self._get_file_ext()
        )


class AaveSource(AaveDailyNode):
    name = NodeNames.aave_raw

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: date, tokens: List[str]) -> Dict[str, pd.DataFrame]:
        df: pd.DataFrame | None = None
        match self.from_config.data_source:
            case DataSource.big_query:
                df = bigquery_aave(self.from_config, day, tokens)
            case DataSource.rpc:
                df = rpc_aave(self.from_config, self.to_path, day, tokens)
            case DataSource.chifra:
                df = chifra_aave(self.from_config, self.to_path, day, tokens)
        df["token"] = df["topics"].apply(lambda x: utils.hex_to_length(x[1], 40))
        tokens_df = {}
        for token_addr, token_df in df.groupby(["token"]):
            clean_df = token_df.drop(columns=["token"])
            tokens_df[token_addr[0]] = clean_df
        return tokens_df

    def _get_file_name(self, param: AaveDailyParam) -> str:
        return (
            f"{self.from_config.chain.name}-aave_v3-{param.token}-{param.day.strftime('%Y-%m-%d')}.raw"
            + self._get_file_ext()
        )

    @property
    def _parse_date_column(self) -> List[str]:
        return ["block_timestamp"]


class SqueethSource(DailyNode):
    name = NodeNames.osqth_raw

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: date):
        df: pd.DataFrame | None = None
        match self.from_config.data_source:
            case DataSource.big_query:
                # df = bigquery_pool(self.from_config, day)
                raise NotImplementedError()
            case DataSource.rpc:
                df = rpc_squeeth(self.from_config, self.to_path, day)
            case DataSource.chifra:
                # df = chifra_pool(self.from_config, self.to_path, day)
                raise NotImplementedError()

        return df

    def _get_file_name(self, param: DailyParam) -> str:
        return (
            f"{self.from_config.chain.name}-squeeth-controller-{param.day.strftime('%Y-%m-%d')}.raw"
            + self._get_file_ext()
        )


class GmxV2Source(DailyNode):
    name = NodeNames.gmx2_raw

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: date):
        if self.config.to_config.to_file_type == ToFileType.csv:
            print("Gmx raw file will be very large in csv format, so it will be saved as feather")
        match self.from_config.data_source:
            case DataSource.rpc:
                df = rpc_gmx_v2(self.from_config, self.to_path, day)
            case _:
                raise NotImplementedError()

        return df

    def _get_file_ext(self):
        return ".feather"

    def _get_file_name(self, param: DailyParam) -> str:
        return f"{self.from_config.chain.name}-GmxV2-{param.day.strftime('%Y-%m-%d')}.raw" + self._get_file_ext()
