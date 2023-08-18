import math
from typing import List

import demeter_fetch._typing as _typing
import demeter_fetch.constants as constants
import numpy as np
from decimal import Decimal


def signed_int(h):
    """
    Converts hex values to signed integers.
    """
    s = bytes.fromhex(h[2:])
    i = int.from_bytes(s, 'big', signed=True)
    return i


def hex_to_address(topic_str):
    return "0x" + topic_str[26:]


def split_topic(value: str) -> List[str]:
    value = value.strip("[]").replace("\"", "").replace("'", "").replace(" ", "").replace("\n", ",")
    return value.split(",")


def handle_proxy_event(topic_str):
    if topic_str is None or isinstance(topic_str, float):  # is none or nan
        return None
    topic_list = split_topic(topic_str)
    type_topic = topic_list[0]
    if len(topic_list) > 1 and (
            type_topic == constants.INCREASE_LIQUIDITY or type_topic == constants.DECREASE_LIQUIDITY or type_topic == constants.COLLECT):
        return int(topic_list[1], 16)
    else:
        return None


def get_tx_type(topics_str):
    topic_list = split_topic(topics_str)
    type_topic = topic_list[0]
    tx_type = constants.type_dict[type_topic]
    return tx_type


def compare_int_with_error(a: int, b: int, error=1) -> bool:
    return abs(a - b) <= error


def compare_burn_data(a: str, b: str) -> bool:
    """
    0x0000000000000000000000000000000000000000000000000014aca30ddf7569
      000000000000000000000000000000000000000000000000000041b051acc70d
      0000000000000000000000000000000000000000000000000000000000000000

    """
    if len(a) != 194 or len(b) != 194:
        return False
    if a[0: 66] != b[0: 66]:
        return False
    if not compare_int_with_error(int("0x" + a[66:66 + 64], 16), int("0x" + b[66:66 + 64], 16)):
        return False
    if not compare_int_with_error(int("0x" + a[66 + 64:66 + 2 * 64], 16), int("0x" + b[66 + 64:66 + 2 * 64], 16)):
        return False
    return True


def handle_event(tx_type, topics_str, data_hex):
    # proprocess topics string ->topic list
    # topics_str = topics.values[0]
    receipt = current_tick = tick_lower = tick_upper = None
    liquidity = Decimal(np.nan)
    delta_liquidity = Decimal(np.nan)
    current_liquidity = Decimal(np.nan)
    sqrtPriceX96 = Decimal(np.nan)

    topic_list = split_topic(topics_str)

    # data_hex = data.values[0]

    no_0x_data = data_hex[2:]
    chunk_size = 64
    chunks = len(no_0x_data)
    match tx_type:
        case _typing.OnchainTxType.SWAP:
            sender = hex_to_address(topic_list[1])
            receipt = hex_to_address(topic_list[2])
            split_data = ["0x" + no_0x_data[i:i + chunk_size] for i in range(0, chunks, chunk_size)]
            amount0, amount1, sqrtPriceX96, current_liquidity, current_tick = [signed_int(onedata) for onedata in split_data]

        case _typing.OnchainTxType.BURN:
            sender = hex_to_address(topic_list[1])
            tick_lower = signed_int(topic_list[2])
            tick_upper = signed_int(topic_list[3])
            split_data = ["0x" + no_0x_data[i:i + chunk_size] for i in range(0, chunks, chunk_size)]
            liquidity, amount0, amount1 = [signed_int(onedata) for onedata in split_data]
            delta_liquidity = -liquidity
        case _typing.OnchainTxType.MINT:
            # sender = topic_str_to_address(topic_list[1])
            owner = hex_to_address(topic_list[1])
            tick_lower = signed_int(topic_list[2])
            tick_upper = signed_int(topic_list[3])
            split_data = ["0x" + no_0x_data[i:i + chunk_size] for i in range(0, chunks, chunk_size)]
            sender = hex_to_address(split_data[0])
            liquidity, amount0, amount1 = [signed_int(onedata) for onedata in split_data[1:]]
            delta_liquidity = liquidity
        case _typing.OnchainTxType.COLLECT:
            tick_lower = signed_int(topic_list[2])
            tick_upper = signed_int(topic_list[3])
            split_data = ["0x" + no_0x_data[i:i + chunk_size] for i in range(0, chunks, chunk_size)]
            sender = hex_to_address(topic_list[1])
            receipt = hex_to_address(split_data[0])
            amount0, amount1 = [signed_int(onedata) for onedata in split_data[1:]]

        case _:
            raise ValueError("not support tx type")
    return sender, receipt, Decimal(amount0), Decimal(amount1), Decimal(sqrtPriceX96), Decimal(current_liquidity), \
        current_tick, tick_lower, tick_upper, Decimal(liquidity), Decimal(delta_liquidity)
