#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-08 12:04
# @Author  : 32ethers
# @Description:
import unittest
from typing import List

from demeter_fetch import DappType, ToType, Config, ToConfig
from demeter_fetch.common import Node
from demeter_fetch.core import get_relative_nodes
from demeter_fetch.core.engine import get_root_node
from demeter_fetch.processor_uniswap import UniTick, UniUserLP
from demeter_fetch.processor_uniswap.relative_price import UniRelativePrice


class TreeTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TreeTest, self).__init__(*args, **kwargs)

    def check_sequence(self, dapp: DappType, to_type: ToType, root_node, squence: List[str]):
        root: Node = get_root_node(dapp, to_type)
        self.assertTrue(type(root) is root_node)
        result = get_relative_nodes(root, config=Config(None, ToConfig(None, "", False, False, False, None)))
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

