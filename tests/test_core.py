#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-08 12:04
# @Author  : 32ethers
# @Description:
import unittest

import pandas as pd
import numpy as np
import os
import sys

from demeter_fetch.core import get_relative_nodes
from demeter_fetch.common import Node


class TreeTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TreeTest, self).__init__(*args, **kwargs)

    def test_find_rel_nodes(self):
        t4: Node = Node([], "t4")
        t5: Node = Node([], "t5")
        t6: Node = Node([], "t6")
        t7: Node = Node([], "t7")
        t8: Node = Node([], "t8")
        t3: Node = Node([t8], "t3")
        t2: Node = Node([t5, t6, t7], "t2")
        t1: Node = Node([t2, t3, t4], "t1")
        result = get_relative_nodes(t1)
        print(result)

    def test_generate_loop_tree(self):
        pool: Node = Node([], "pool")
        proxy_lp: Node = Node([], "proxy_lp")
        tick: Node = Node([pool, proxy_lp], "tick")
        tx: Node = Node([tick], "tx")
        user_tick: Node = Node([tx, tick], "user_tick")
        position: Node = Node([user_tick], "position")
        user_lp: Node = Node([position], "user_lp")
        result1 = get_relative_nodes(user_lp)
        user_tick: Node = Node([tick, tx], "user_tick")
        result2 = get_relative_nodes(user_lp)

        print(result1)
        self.assertTrue(result2 == result1)
        # [proxy_lp, pool, tick, proxy_lp, pool, tick, tx, user_tick, position, user_lp]
