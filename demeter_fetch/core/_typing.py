#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-08 11:36
# @Author  : 32ethers
# @Description:
from dataclasses import dataclass
from datetime import date
from typing import List, Callable, Dict, TypeVar, Generic, Union, Any

import pandas as pd

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
    # config, param{NodeName, List[files]}, Node
    processor: Callable[[Config, date, Dict[str, List[str] | str], None], Any]
    file_name: Callable[[FromConfig, str | None], str]
    load_converter: Dict | None = None
    is_daily: bool = False

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name
