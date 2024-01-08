#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-08 11:36
# @Author  : 32ethers
# @Description:
from dataclasses import dataclass
from typing import List, Callable

import pandas as pd
import numpy as np
import os
import sys

from demeter_fetch import Config


@dataclass
class DescDataFrame:
    id: str
    columns: List[str]
    df: pd.DataFrame


@dataclass
class Node:
    name: str
    depend: List  # list of node
    processor: Callable[[Config, List[DescDataFrame]], DescDataFrame]

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name
