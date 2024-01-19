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
from .. import DappType, ToType, UniNodesNames, AaveNodesNames
from ..common import DataSource, TimeUtil, to_decimal
from ..processor_uniswap.tick import get_pool_tick_df, get_tick_df
from ..processor_uniswap.minute import get_minute_df
from ..sources import uni_source_pool, uni_source_proxy_transfer, uni_source_proxy_lp, aave_source


class UniNodes:
    pool = Node(
        name=UniNodesNames.pool,
        depend=[],
        processor=uni_source_pool,
        file_name=lambda cfg, day: f"{cfg.chain.name}-{cfg.uniswap_config.pool_address}-{day}.raw.csv",
        is_daily=True,
    )
    proxy_transfer = Node(
        name=UniNodesNames.proxy_transfer,
        depend=[],
        processor=uni_source_proxy_transfer,
        file_name=lambda cfg, day: f"{cfg.chain.name}-uniswap-proxy-transfer-{day}.raw.csv",
        is_daily=True,
    )
    proxy_lp = Node(
        name=UniNodesNames.proxy_lp,
        depend=[],
        processor=uni_source_proxy_lp,
        file_name=lambda cfg, day: f"{cfg.chain.name}-uniswap-proxy-lp-{day}.raw.csv",
        is_daily=True,
    )

    minute = Node(
        name=UniNodesNames.minute,
        depend=[pool],
        processor=get_minute_df,
        file_name=lambda cfg, day: f"{cfg.chain.name}-{cfg.uniswap_config.pool_address}-{day}.minute.csv",
        load_converter={
            "inAmount0": to_decimal,
            "inAmount1": to_decimal,
            "currentLiquidity": to_decimal,
            "netAmount0": to_decimal,
            "netAmount1": to_decimal,
        },
    )
    tick = Node(
        name=UniNodesNames.tick,
        depend=[pool, proxy_lp],
        processor=get_tick_df,
        file_name=lambda cfg, day: f"{cfg.chain.name}-{cfg.uniswap_config.pool_address}-{day}.tick.csv",
        load_converter={
            "amount0": to_decimal,
            "amount1": to_decimal,
            "total_liquidity": to_decimal,
            "total_liquidity_delta": to_decimal,
            "sqrtPriceX96": to_decimal,
        },
    )
    tick_without_pos = Node(
        name=UniNodesNames.tick_without_pos,
        depend=[pool],
        processor=get_pool_tick_df,
        file_name=lambda cfg, day: f"{cfg.chain.name}-{cfg.uniswap_config.pool_address}-{day}.pool.tick.csv",
        load_converter={
            "amount0": to_decimal,
            "amount1": to_decimal,
            "total_liquidity": to_decimal,
            "total_liquidity_delta": to_decimal,
            "sqrtPriceX96": to_decimal,
        },
    )

    positions = Node(
        name=UniNodesNames.positions,
        depend=[tick],
        processor=lambda cfg, day, data: "positions",
        file_name=lambda cfg, day: f"{cfg.chain.name}-{cfg.uniswap_config.pool_address}-{day}.position.csv",
        load_converter=None,
    )
    addresses = Node(
        name=UniNodesNames.addresses,
        depend=[positions, tick_without_pos],
        processor=lambda cfg, day, data: "addresses",
        file_name=lambda cfg, day: f"{cfg.chain.name}-{cfg.uniswap_config.pool_address}-{day}.address.csv",
        load_converter=None,
    )


class AaveNodes:
    pool = Node(
        name=AaveNodesNames.pool,
        depend=[],
        processor=aave_source,
        file_name=lambda cfg, day: "",
        is_daily=True,
    )

    minute = Node(
        name=AaveNodesNames.minute,
        depend=[pool],
        processor=lambda cfg, day, data: "minute",
        file_name=lambda cfg, day: "",
        csv_converter=None,
    )
    tick = Node(
        name=AaveNodesNames.tick,
        depend=[pool],
        processor=lambda cfg, day, data: "tick",
        file_name=lambda cfg, day: "",
        csv_converter=None,
    )


def get_root_node(dapp: DappType, to_type: ToType, ignore_pos_id: bool = False) -> Node:
    if dapp == DappType.uniswap:
        match to_type:
            case ToType.raw:
                return UniNodes.pool
            case ToType.tick:
                if ignore_pos_id:
                    return UniNodes.tick_without_pos
                else:
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
