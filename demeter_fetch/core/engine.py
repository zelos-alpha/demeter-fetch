#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-08 14:22
# @Author  : 32ethers
# @Description:
from typing import List

from .. import DappType, ToType
from ..common import Node
from ..processor_aave import AaveMinute, AaveTick
from ..processor_gmx2 import GmxV2Tick, GmxV2Price, GmxV2PoolTx, GmxV2Minute
from ..processor_squeeth import SqueethMinute
from ..processor_uniswap import UniUserLP, UniPositions, UniTick, UniTickNoPos, UniMinute, UniV4Minute, UniV4Tick
from ..processor_uniswap.relative_price import UniRelativePrice
from ..sources import (
    UniSourcePool,
    UniSourceProxyTransfer,
    UniSourceProxyLp,
    AaveSource,
    UniTransaction,
    SqueethSource,
    UniV4SourcePool,
    GmxV2Source,
)


def _get_reversed_copy(list_to_reverse):
    ret_list = list_to_reverse.copy()
    ret_list.reverse()
    return ret_list


def get_relative_nodes(root: Node) -> List[Node]:
    depth_first_array = []
    stack = [root]
    while len(stack) > 0:
        current_node: Node = stack.pop()
        depth_first_array.append(current_node)
        current_node_depends = []
        for depend_class in current_node.__class__.depend:
            depend_configs: List = current_node.get_config_for_depend(depend_class.name)
            for depend_config in depend_configs:
                depend_instance = depend_class()
                depend_instance.set_config(depend_config)
                depend_instance.id = depend_config.id
                # if depend_instance not in stack and depend_instance not in depth_first_array:
                stack.append(depend_instance)
                current_node_depends.append(depend_instance)
        current_node.set_depend_instance(current_node_depends)
    depth_first_array.reverse()
    final_list = []
    for node in depth_first_array:
        if node not in final_list:
            final_list.append(node)

    return final_list


# region depends

# uniswap
UniSourcePool.depend = []
UniSourceProxyTransfer.depend = []
UniSourceProxyLp.depend = []
UniMinute.depend = [UniSourcePool]
UniTick.depend = [UniSourcePool, UniSourceProxyLp]
UniTickNoPos.depend = [UniSourcePool]
UniTransaction.depend = [UniTick]
UniPositions.depend = [UniTick, UniTransaction]
UniUserLP.depend = [UniPositions]
UniRelativePrice.depend = [UniTickNoPos]
# AAVE
AaveSource.depend = []
AaveMinute.depend = [AaveSource]
AaveTick.depend = [AaveSource]

# SQUEETH
SqueethSource.depend = []
SqueethMinute.depend = [SqueethSource, UniRelativePrice]

# Uniswap V4
UniV4SourcePool.depend = []
UniV4Minute.depend = [UniV4SourcePool]
UniV4Tick.depend = [UniV4SourcePool]

# Gmx
GmxV2Source.depend = []
GmxV2Tick.depend = [GmxV2Source]
GmxV2Price.depend = [GmxV2Source]
GmxV2PoolTx.depend = [GmxV2Tick]
GmxV2Minute.depend = [GmxV2Price, GmxV2PoolTx]


def get_root_node(dapp: DappType, to_type: ToType, ignore_pos_id: bool = False) -> Node:
    if dapp == DappType.uniswap:
        match to_type:
            case ToType.raw:
                return UniSourcePool()
            case ToType.tick:
                if ignore_pos_id:
                    return UniTickNoPos()
                else:
                    return UniTick()
            case ToType.position:
                return UniPositions()
            case ToType.minute:
                return UniMinute()
            case ToType.user_lp:
                return UniUserLP()
            case ToType.price:
                return UniRelativePrice()
            case _:
                raise NotImplemented(f"{dapp} {to_type} not supported")

    elif dapp == DappType.aave:
        match to_type:
            case ToType.raw:
                return AaveSource()
            case ToType.minute:
                return AaveMinute()
            case ToType.tick:
                return AaveTick()
            case _:
                raise NotImplemented(f"{dapp} {to_type} not supported")
    elif dapp == DappType.squeeth:
        match to_type:
            case ToType.raw:
                return SqueethSource()
            case ToType.minute:
                return SqueethMinute()
    elif dapp == DappType.uniswap_v4:
        match to_type:
            case ToType.raw:
                return UniV4SourcePool()
            case ToType.minute:
                return UniV4Minute()
            case ToType.tick:
                return UniV4Tick()
    elif dapp == DappType.gmx_v2:
        match to_type:
            case ToType.raw:
                return GmxV2Source()
            case ToType.tick:
                return GmxV2Tick()
            case ToType.price:
                return GmxV2Price()
            case ToType.pool:
                return GmxV2PoolTx()
            case ToType.minute:
                return GmxV2Minute()
    else:
        raise NotImplemented(f"{dapp} not supported")
