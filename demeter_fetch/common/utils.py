import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pandas as pd
import requests
import numpy as np

from demeter_fetch.common._typing import *


global_pbar = None


def set_global_pbar(pbar):
    global global_pbar
    global_pbar = pbar


def get_depend_name(depend_name, id):
    if id == "":
        return depend_name
    else:
        return f"{depend_name}:{id}"


def print_log(*args, tqdm_bar=None):
    if global_pbar is not None:
        tqdm_bar = global_pbar
    if tqdm_bar is not None:
        msg = str(*args).lstrip("(").rstrip(")")
        tqdm_bar.display(str(tqdm_bar) + f"    {str(msg)}")
        pass
    else:
        new_tuple = (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),)
        new_tuple = new_tuple + args
        print(*new_tuple)


class TextUtil(object):
    @staticmethod
    def cut_after(text: str, symbol: str) -> str:
        index = text.find(symbol)
        return text[0:index]


class TimeUtil(object):
    @staticmethod
    def get_minute(time: datetime) -> datetime:
        return datetime(time.year, time.month, time.day, time.hour, time.minute, 0)

    @staticmethod
    def get_date_array(date_begin, date_end) -> List[date]:
        return [date_begin + timedelta(days=x) for x in range(0, 1 + (date_end - date_begin).days)]


class HexUtil(object):
    @staticmethod
    def to_signed_int(h):
        """
        Converts hex values to signed integers.
        """
        s = bytes.fromhex(h[2:])
        i = int.from_bytes(s, "big", signed=True)
        return i


def to_decimal(value):
    return Decimal(value) if value else Decimal(0)


def to_int(value):
    return int(value) if value else int(0)


# class DataUtil(object):
#     @staticmethod
#     def fill_missing(data_list: List[MinuteData]) -> List[MinuteData]:
#         if len(data_list) < 1:
#             return data_list
#         # take the first minute in data. instead of 0:00:00
#         # so here will be a problem, if the first data is 0:03:00, the first 2 minutes will be blank
#         # that's because there is no previous data to follow
#         # those empty rows will be filled in loading stage
#         index_minute = data_list[0].timestamp
#         new_list = []
#         data_list_index = 0
#
#         start_day = data_list[0].timestamp.day
#         while index_minute.day == start_day:
#             if (data_list_index < len(data_list)) and (index_minute == data_list[data_list_index].timestamp):
#                 item = data_list[data_list_index]
#                 data_list_index += 1
#             else:
#                 item = MinuteData()
#                 item.timestamp = index_minute
#             prev_data = new_list[len(new_list) - 1] if len(new_list) - 1 >= 0 else None
#             # if no previous(this might happen in the first minutes) data, this row will be discarded
#             if item.fill_missing_field(prev_data):
#                 new_list.append(item)
#             index_minute = index_minute + timedelta(minutes=1)
#
#         return new_list


class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(obj, date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)


def hex_to_length(hex_str: str, new_length: int):
    """
    expend hex string to new length, will keep leading 0x if it exists
    eg:
    1. hex_str=0x1, length=3, result is 0x001
    2. hex_str=0x001, length=2, result is 0x01
    """

    has_0x = hex_str[0:2] == "0x" if len(hex_str) >= 2 else False

    hex_without_0x = hex_str if not has_0x else hex_str[2:]

    if len(hex_without_0x) <= new_length:
        return hex_without_0x.zfill(new_length) if not has_0x else "0x" + hex_without_0x.zfill(new_length)
    else:
        if hex_without_0x[-new_length - 1] != "0":
            raise RuntimeError("Not enough leading zeros to remove")
        return hex_without_0x[-new_length:] if not has_0x else "0x" + hex_without_0x[-new_length:]


def split_topic(value: str | list) -> List[str]:
    if isinstance(value, list):
        return value
    elif isinstance(value, np.ndarray):
        return list(value)
    elif isinstance(value, str):
        value = value.strip("[]").replace('"', "").replace("'", "").replace(" ", "").replace("\n", ",")
        return value.split(",")
    else:
        raise RuntimeError("Unknown topic type")


def get_tx_type(topics_str):
    if not isinstance(topics_str, list) and not isinstance(topics_str, np.ndarray) and pd.isna(topics_str):
        return topics_str
    topic_list = split_topic(topics_str)
    type_topic = topic_list[0]
    tx_type = KECCAK(type_topic)
    return tx_type


def get_transfer_from_logs(df: pd.DataFrame) -> pd.DataFrame:
    logs = df[["transaction_hash", "log_address", "topics", "data"]]
    logs["topics"] = logs["topics"].apply(split_topic)
    logs["topic0"] = logs["topics"].apply(lambda x: x[0])
    logs = logs[logs["topic0"] == KECCAK.TRANSFER.value]
    logs["from"] = logs["topics"].apply(lambda x: hex_to_length(x[1], 40))
    logs["to"] = logs["topics"].apply(lambda x: hex_to_length(x[2], 40))
    logs["value"] = logs["data"].apply(lambda x: Decimal(int(x, 16) if isinstance(x, str) else 0))
    logs = logs.drop(columns=["topics", "topic0", "data"])
    return logs


class ApiUtil:
    @staticmethod
    def query_blockno_from_time(
        chain: ChainType, blk_time: datetime, is_before: bool = True, proxy="", etherscan_api_key=None
    ):
        proxies = (
            {
                "http": proxy,
                "https": proxy,
            }
            if proxy
            else {}
        )
        before_or_after = "before" if is_before else "after"
        url = ChainTypeConfig[chain]["query_height_api"]
        blk_time = blk_time.replace(tzinfo=timezone.utc)
        url = url.replace("%1", str(int(blk_time.timestamp()))).replace("%2", before_or_after)
        if etherscan_api_key is not None and etherscan_api_key != "":
            url += "&apikey=" + etherscan_api_key
        retry = 3
        result = None
        while retry:
            try:
                result = requests.get(url, proxies=proxies)
                retry = 0
            except Exception as ex:
                print("Query etherscan failed, error: " + str(ex))
                retry -= 1
        if not result:
            raise RuntimeError("request error with retry 3 times.")
        if result.status_code != 200:
            raise RuntimeError("request block number failed, code: " + str(result.status_code))
        result_json = result.json()
        if int(result_json["status"]) == 1:
            return int(result_json["result"])
        else:
            raise RuntimeError("request block number failed, message: " + str(result_json))
