#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-09 11:21
# @Author  : 32ethers
# @Description:

import pandas as pd
import numpy as np
import os
import sys
from ._typing import *
from .. import DappType, ToType
from ..common import DataSource
from ..sources import uni_source_pool, uni_source_proxy_transfer, uni_source_proxy_lp, aave_source


class UniNodes:
    pool = Node(
        name="pool",
        depend=[],
        processor=uni_source_pool,
        file_name=lambda cfg, day: f"{cfg.chain.name}-{cfg.uniswap_config.pool_address}-{day}.raw.csv",
        is_download=True,
    )
    proxy_transfer = Node(
        name="proxy_transfer",
        depend=[],
        processor=uni_source_proxy_transfer,
        file_name=lambda cfg, day: f"{cfg.chain.name}-uniswap-proxy-transfer-{day}.raw.csv",
        is_download=True,
    )
    proxy_lp = Node(
        name="proxy_LP",
        depend=[],
        processor=uni_source_proxy_lp,
        file_name=lambda cfg, day: f"{cfg.chain.name}-uniswap-proxy-lp-{day}.raw.csv",
        is_download=True,
    )

    minute = Node(
        name="minute",
        depend=[pool],
        processor=lambda cfg, day, data: "minute",
        file_name=lambda cfg, day: f"{cfg.chain.name}-{cfg.uniswap_config.pool_address}-{day}.minute.csv",
    )
    tick = Node(
        name="tick",
        depend=[pool, proxy_lp],
        processor=lambda cfg, day, data: "tick",
        file_name=lambda cfg, day: f"{cfg.chain.name}-{cfg.uniswap_config.pool_address}-{day}.tick.csv",
    )
    tick_without_pos = Node(
        name="tick_without_pos",
        depend=[pool],
        processor=lambda cfg, day, data: "tick_without_pos",
        file_name=lambda cfg, day: f"{cfg.chain.name}-{cfg.uniswap_config.pool_address}-{day}.pool-tick.csv",
    )

    positions = Node(
        name="Positions",
        depend=[tick],
        processor=lambda cfg, day, data: "positions",
        file_name=lambda cfg, day: f"{cfg.chain.name}-{cfg.uniswap_config.pool_address}-{day}.position.csv",
    )
    addresses = Node(
        name="Addresses",
        depend=[positions, tick_without_pos],
        processor=lambda cfg, day, data: "addresses",
        file_name=lambda cfg, day: f"{cfg.chain.name}-{cfg.uniswap_config.pool_address}-{day}.address.csv",
    )


class AaveNodes:
    pool = Node(
        name="pool",
        depend=[],
        processor=aave_source,
        file_name=lambda cfg, day: "",
        is_download=True,
    )

    minute = Node(
        name="minute",
        depend=[pool],
        processor=lambda cfg, day, data: "minute",
        file_name=lambda cfg, day: "",
    )
    tick = Node(
        name="tick",
        depend=[pool],
        processor=lambda cfg, day, data: "tick",
        file_name=lambda cfg, day: "",
    )


def get_root_node(dapp: DappType, to_type: ToType) -> Node:
    if dapp == DappType.uniswap:
        match to_type:
            case ToType.raw:
                return UniNodes.pool
            case ToType.tick:
                return UniNodes.tick
            case ToType.position:
                return UniNodes.positions
            case ToType.minute:
                return UniNodes.minute
            case ToType.address:
                return UniNodes.addresses
            case _:
                raise NotImplemented(f"{dapp} {to_type} not supported")

    elif dapp == DappType.aave:
        match to_type:
            case ToType.raw:
                return AaveNodes.pool
            case ToType.minute:
                return AaveNodes.minute
            case ToType.tick:
                return AaveNodes.tick

    else:
        raise NotImplemented(f"{dapp} not supported")
