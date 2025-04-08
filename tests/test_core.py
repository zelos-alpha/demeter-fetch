#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-08 12:04
# @Author  : 32ethers
# @Description:
import unittest
from typing import List

from demeter_fetch import (
    DappType,
    ToType,
    Config,
    ToConfig,
    FromConfig,
    ChainType,
    DataSource,
    UniswapConfig,
    TokenConfig,
)
from demeter_fetch.common import Node
from demeter_fetch.core import get_relative_nodes
from demeter_fetch.core.engine import get_root_node
from demeter_fetch.processor_gmx2 import GmxV2Minute
from demeter_fetch.processor_squeeth import SqueethMinute
from demeter_fetch.processor_uniswap import UniTick, UniUserLP
from demeter_fetch.processor_uniswap.relative_price import UniRelativePrice


class TreeTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TreeTest, self).__init__(*args, **kwargs)

    def check_sequence(self, dapp: DappType, to_type: ToType, root_node, squence: List[str]):
        root: Node = get_root_node(dapp, to_type)
        root.set_config(
            Config(
                FromConfig(
                    chain=ChainType.ethereum,
                    data_source=DataSource.rpc,
                    dapp_type=DappType.uniswap,
                    start=None,
                    end=None,
                    uniswap_config=UniswapConfig("", False, TokenConfig("a", 6), TokenConfig("b", 6), True),
                ),
                ToConfig(
                    None,
                    "",
                    False,
                    False,
                    False,
                    None,
                ),
            ),
        )
        self.assertTrue(type(root) is root_node)
        result = get_relative_nodes(
            root,
        )
        result = [n.name for n in result]
        print(result)
        self.assertEqual(result, squence)

    def test_uni_tick(self):
        self.check_sequence(DappType.uniswap, ToType.tick, UniTick, ["uni_pool", "uni_proxy_LP", "uni_tick"])

    def test_price(self):
        self.check_sequence(
            DappType.uniswap, ToType.price, UniRelativePrice, ["uni_pool", "uni_tick_without_pos", "uni_rel_price"]
        )

    def test_user_lp(self):
        self.check_sequence(
            DappType.uniswap,
            ToType.user_lp,
            UniUserLP,
            ["uni_pool", "uni_proxy_LP", "uni_tick", "uni_tx", "uni_positions", "uni_user_lp"],
        )
    def test_gmx2_minute(self):
        self.check_sequence(
            DappType.gmx_v2,
            ToType.minute,
            GmxV2Minute,
            ["gmx2_raw", "gmx2_price", "gmx2_tick", "gmx2_pool", "gmx2_minute"],
        )
    def test_osqth_minute(self):
        # requires two extra token price node
        self.check_sequence(
            DappType.squeeth,
            ToType.minute,
            SqueethMinute,
            [
                "osqth_raw",
                "uni_pool",
                "uni_tick_without_pos",
                "uni_rel_price",
                "uni_pool",
                "uni_tick_without_pos",
                "uni_rel_price",
                "osqth_minute",
            ],
        )
