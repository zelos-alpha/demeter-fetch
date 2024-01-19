#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-10 10:44
# @Author  : 32ethers
# @Description:
from dataclasses import dataclass
from datetime import date
from typing import Dict, List

import pandas as pd
import numpy as np
import os
import sys

from ..common import Config, DataSource
from .big_query import bigquery_aave, bigquery_pool, bigquery_proxy_lp, bigquery_proxy_transfer
from .rpc import rpc_pool, rpc_proxy_lp, rpc_proxy_transfer
from .chifra import chifra_pool, chifra_proxy_lp, chifra_proxy_transfer
from ..core import Node


@dataclass
class EventLog:
    block_number = 0
    block_timestamp = 0
    transaction_hash = 0
    transaction_index = 0
    log_index = 0
    topics = 0
    data = 0


def uni_source_pool(cfg: Config, day: date, input_files: Dict[str, List[str]], node):
    day_str = day.strftime("%Y-%m-%d")
    df: pd.DataFrame | None = None
    match cfg.from_config.data_source:
        case DataSource.big_query:
            df = bigquery_pool(cfg.from_config, day)
        case DataSource.rpc:
            df = rpc_pool(cfg.from_config, cfg.to_config.save_path, day)
        case DataSource.chifra:
            df = chifra_pool(cfg.from_config, cfg.to_config.save_path, day)
    df.to_csv(os.path.join(cfg.to_config.save_path, node.file_name(cfg.from_config, day_str)), index=False)


def uni_source_proxy_lp(cfg: Config, day: date, input_files: Dict[str, List[str]], node):
    day_str = day.strftime("%Y-%m-%d")
    df: pd.DataFrame | None = None
    match cfg.from_config.data_source:
        case DataSource.big_query:
            df = bigquery_proxy_lp(cfg.from_config, day)
        case DataSource.rpc:
            df = rpc_proxy_lp(cfg.from_config, cfg.to_config.save_path, day)
        case DataSource.chifra:
            df = chifra_proxy_lp(cfg.from_config, cfg.to_config.save_path, day)
    df.to_csv(os.path.join(cfg.to_config.save_path, node.file_name(cfg.from_config, day_str)), index=False)


def uni_source_proxy_transfer(cfg: Config, day: date, input_files: Dict[str, List[str]], node):
    day_str = day.strftime("%Y-%m-%d")
    df: pd.DataFrame | None = None
    match cfg.from_config.data_source:
        case DataSource.big_query:
            return bigquery_proxy_transfer(cfg.from_config, day)
        case DataSource.rpc:
            df = rpc_proxy_transfer(cfg.from_config, cfg.to_config.save_path, day)
        case DataSource.chifra:
            df = chifra_proxy_transfer(cfg.from_config, cfg.to_config.save_path, day)
    df.to_csv(os.path.join(cfg.to_config.save_path, node.file_name(cfg.from_config, day_str)), index=False)


def aave_source(cfg: Config, day: date, data):
    match cfg.from_config.data_source:
        case DataSource.big_query:
            return bigquery_aave(cfg, day)
