#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-08 11:36
# @Author  : 32ethers
# @Description:
from dataclasses import dataclass
from datetime import date
from typing import List, Callable, Dict, TypeVar, Generic, Union

import pandas as pd
import numpy as np
import os
import sys

from demeter_fetch import Config, FromConfig

T = TypeVar("T")


@dataclass
class DescDataFrame(Generic[T]):
    df: Union[pd.DataFrame, List[T]]

    def __str__(self):
        return str(self.df)

    def __repr__(self):
        return str(self.df)


@dataclass
class Node:
    name: str
    depend: List  # list of node
    processor: Callable[[Config, date, Dict[str, DescDataFrame]], DescDataFrame]
    file_name: Callable[[FromConfig, str], str]
    is_download: bool = False

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name
