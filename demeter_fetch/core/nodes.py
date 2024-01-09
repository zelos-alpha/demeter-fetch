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


class UniNodes:
    pool = Node("pool", [], None, True)
    proxy_transfer = Node("proxy_transfer", [], None, True)
    proxy_lp = Node("proxy_LP", [], None, True)

    minute = Node("minute", [pool], None)
    raw = Node("raw", [pool], None)
    tick = Node("tick", [pool, proxy_lp], None)
    tick_without_pos = Node("tick_without_pos", [pool], None)

    positions = Node("Positions", [tick], None)
    addresses = Node("Addresses", [positions, tick_without_pos], None)


class AaveNodes:
    pool = Node("pool", [], None, True)
    raw = Node("raw", [pool], None)
    minute = Node("minute", [pool], None)
    tick = Node("tick", [pool], None)


def get_root_node(dapp: DappType, to_type: ToType) -> Node:
    if dapp == DappType.uniswap:
        match to_type:
            case ToType.raw:
                return UniNodes.raw
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
                return AaveNodes.raw
            case ToType.minute:
                return AaveNodes.minute
            case ToType.tick:
                return AaveNodes.tick

    else:
        raise NotImplemented(f"{dapp} not supported")
