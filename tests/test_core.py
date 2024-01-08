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

from demeter_fetch.core import Node, generate_tree

t4: Node = Node("t4", [], lambda a, b: print(a))
t5: Node = Node("t5", [], lambda a, b: print(a))
t6: Node = Node("t6", [], lambda a, b: print(a))
t7: Node = Node("t7", [], lambda a, b: print(a))
t8: Node = Node("t8", [], lambda a, b: print(a))

t3: Node = Node("t3", [t8], lambda a, b: print(a))
t2: Node = Node("t2", [t5, t6, t7], lambda a, b: print(a))
t1: Node = Node("t1", [t2, t3, t4], lambda a, b: print(a))


class TreeTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TreeTest, self).__init__(*args, **kwargs)

    def test_find_rel_nodes(self):
        result = generate_tree(t1)
        print(result)
