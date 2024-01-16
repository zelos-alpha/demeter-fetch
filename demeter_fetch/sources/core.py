#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-10 10:44
# @Author  : 32ethers
# @Description:
from dataclasses import dataclass
from datetime import date
from typing import Dict

import pandas as pd
import numpy as np
import os
import sys

from ..common import Config, DataSource
from .big_query import bigquery_aave, bigquery_pool, bigquery_proxy_lp, bigquery_proxy_transfer
from .rpc import rpc_pool, rpc_proxy_lp,rpc_proxy_transfer


@dataclass
class EventLog:
    block_number = 0
    block_timestamp = 0
    transaction_hash = 0
    transaction_index = 0
    log_index = 0
    topics = 0
    data = 0


def uni_source_pool(cfg: Config, day: date, data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    df: pd.DataFrame | None = None
    match cfg.from_config.data_source:
        case DataSource.big_query:
            df = bigquery_pool(cfg.from_config, day)
        case DataSource.rpc:
            df = rpc_pool(cfg.from_config, cfg.to_config.save_path, day)
        case DataSource.chifra:
            raise NotImplemented()
    return df


def uni_source_proxy_lp(cfg: Config, day: date, data) -> pd.DataFrame:
    df: pd.DataFrame | None = None
    match cfg.from_config.data_source:
        case DataSource.big_query:
            df = bigquery_proxy_lp(cfg.from_config, day)
        case DataSource.rpc:
            df = rpc_proxy_lp(cfg.from_config, cfg.to_config.save_path, day)
        case DataSource.chifra:
            raise NotImplemented()
    return df


def uni_source_proxy_transfer(cfg: Config, day: date, data):
    df: pd.DataFrame | None = None
    match cfg.from_config.data_source:
        case DataSource.big_query:
            return bigquery_proxy_transfer(cfg.from_config, day)
        case DataSource.rpc:
            df = rpc_proxy_transfer(cfg.from_config, cfg.to_config.save_path, day)
        case DataSource.chifra:
            raise NotImplemented()
    return df

def aave_source(cfg: Config, day: date, data):
    match cfg.from_config.data_source:
        case DataSource.big_query:
            return bigquery_aave(cfg, day)
