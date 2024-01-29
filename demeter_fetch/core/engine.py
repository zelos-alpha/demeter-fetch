#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-08 14:22
# @Author  : 32ethers
# @Description:
from typing import List

from ..common import Node
from ..processor_aave.minute import AaveMinute
from ..processor_uniswap import UniUserLP, UniPositions, UniTick, UniTickNoPos, UniMinute

from ..sources import UniSourcePool, UniSourceProxyTransfer, UniSourceProxyLp, AaveSource, UniTransaction
from .. import DappType, ToType, UniNodesNames, AaveNodesNames


def _get_reversed_copy(list_to_reverse):
    ret_list = list_to_reverse.copy()
    ret_list.reverse()
    return ret_list


def get_relative_nodes(root: Node) -> List[Node]:
    depth_first_array = []
    stack = [root]
    while len(stack) > 0:
        poped: Node = stack.pop()
        depth_first_array.append(poped)
        if len(poped.depends) > 0:
            # if you want to use a list as a stack, you have to add elements in the tail
            stack.extend(_get_reversed_copy(poped.depends))
    depth_first_array.reverse()
    no_dup_array = []
    for n in depth_first_array:
        if n not in no_dup_array:
            no_dup_array.append(n)
    return no_dup_array


class UniNodes:
    pool = UniSourcePool([])
    proxy_transfer = UniSourceProxyTransfer([])
    proxy_lp = UniSourceProxyLp([])
    minute = UniMinute([pool])
    tick = UniTick([pool, proxy_lp])
    tick_without_pos = UniTickNoPos([pool])
    tx = UniTransaction([tick])
    position = UniPositions([tick, tx])
    user_lp = UniUserLP([position])


class AaveNodes:
    raw = AaveSource([])

    minute = AaveMinute([raw])
    tick = Node([raw])


def get_root_node(dapp: DappType, to_type: ToType, ignore_pos_id: bool = False) -> Node:
    if dapp == DappType.uniswap:
        match to_type:
            case ToType.raw:
                return UniNodes.pool
            case ToType.tick:
                if ignore_pos_id:
                    return UniNodes.tick_without_pos
                else:
                    return UniNodes.tick
            case ToType.position:
                return UniNodes.position
            case ToType.minute:
                return UniNodes.minute
            case ToType.user_lp:
                return UniNodes.user_lp
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
            case _:
                raise NotImplemented(f"{dapp} {to_type} not supported")
    else:
        raise NotImplemented(f"{dapp} not supported")
