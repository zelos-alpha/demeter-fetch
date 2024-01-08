#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-08 14:22
# @Author  : 32ethers
# @Description:
from typing import List

import pandas as pd
import numpy as np
import os
import sys
from ._typing import Node


def _get_reversed_copy(l):
    l1 = l.copy()
    l1.reverse()
    return l1


def generate_tree(root: Node) -> List[Node]:
    depth_first_array = []
    stack = [root]
    while len(stack) > 0:
        poped: Node = stack.pop()
        depth_first_array.append(poped)
        if len(poped.depend) > 0:
            # if you want to use a list as a stack, you have to add elements in the tail
            stack.extend(_get_reversed_copy(poped.depend))
    depth_first_array.reverse()
    return depth_first_array
