import json
import os
from datetime import datetime, timedelta

from ._typing import *


def get_file_name(chain: ChainType, pool_address, day: date):
    return f"{chain.name}-{pool_address}-{day.strftime('%Y-%m-%d')}.raw.csv"

def get_aave_file_name(chain: ChainType, token_address, day: date):
    return f"{chain.name}-aave_v3-{token_address}-{day.strftime('%Y-%m-%d')}.raw.csv"

def convert_raw_file_name(file: str, to_config: ToConfig) -> str:
    file_name = os.path.basename(file)
    file_name_and_ext = os.path.splitext(file_name)

    return os.path.join(
        to_config.save_path,
        f"{file_name_and_ext[0].replace('.raw', '')}.{to_config.type.name}{file_name_and_ext[1]}",
    )

def print_log(*args):
    new_tuple = (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),)
    new_tuple = new_tuple + args
    print(*new_tuple)



def convert_to_config(conf_file: dict) -> Config:
    to_type = ToType[conf_file["to"]["type"]]
    save_path = "../"
    if "save_path" in conf_file["to"]:
        save_path = conf_file["to"]["save_path"]
    multi_process = False
    if "multi_process" in conf_file["to"]:
        multi_process = conf_file["to"]["multi_process"]
    skip_existed = False
    if "skip_existed" in conf_file["to"]:
        skip_existed = conf_file["to"]["skip_existed"]
    to_config = ToConfig(to_type, save_path, multi_process, skip_existed)

    chain = ChainType[conf_file["from"]["chain"]]
    data_source = DataSource[conf_file["from"]["datasource"]]

    dapp_type = DappType[conf_file["from"]["dapp_type"]]

    from_config = FromConfig(chain, data_source, dapp_type)

    if dapp_type == DappType.uniswap:
        pool_address = conf_file["from"]["uniswap"]["pool_address"].lower()
        from_config.uniswap_config = UniswapConfig(pool_address)
    elif dapp_type == DappType.aave:
        token_addresses = [x.lower() for x in conf_file["from"]["aave"]["tokens"]]
        from_config.aave_config = AaveConfig(token_addresses)

    if data_source not in ChainTypeConfig[from_config.chain]["allow"]:
        raise RuntimeError(f"{data_source.name} is not allowed to download from {from_config.chain.name}")
    match data_source:
        case DataSource.file:
            if "file" not in conf_file["from"]:
                raise RuntimeError("should have [from.file]")
            files = None
            if "files" in conf_file["from"]["file"]:
                files = conf_file["from"]["file"]["files"]
            folder = None
            if "folder" in conf_file["from"]["file"]:
                folder = conf_file["from"]["file"]["folder"]
            if files is None and folder is None:
                raise RuntimeError("file_path and folder can not both null")

            from_config.file = FileConfig(files, folder)

        case DataSource.rpc:
            if "rpc" not in conf_file["from"]:
                raise RuntimeError("should have [from.rpc]")
            auth_string = None
            if "auth_string" in conf_file["from"]["rpc"]:
                auth_string = conf_file["from"]["rpc"]["auth_string"]
            http_proxy = None
            if "http_proxy" in conf_file["from"]["rpc"]:
                http_proxy = conf_file["from"]["rpc"]["http_proxy"]
            keep_tmp_files = None
            if "keep_tmp_files" in conf_file["from"]["rpc"]:
                keep_tmp_files = conf_file["from"]["rpc"]["keep_tmp_files"]
            ignore_position_id = False
            if "ignore_position_id" in conf_file["from"]["rpc"]:
                ignore_position_id = conf_file["from"]["rpc"]["ignore_position_id"]
            end_point = conf_file["from"]["rpc"]["end_point"]
            start_time = datetime.strptime(conf_file["from"]["rpc"]["start"], "%Y-%m-%d").date()
            end_time = datetime.strptime(conf_file["from"]["rpc"]["end"], "%Y-%m-%d").date()
            batch_size = int(conf_file["from"]["rpc"]["batch_size"])

            from_config.rpc = RpcConfig(
                end_point=end_point,
                start=start_time,
                end=end_time,
                batch_size=batch_size,
                auth_string=auth_string,
                http_proxy=http_proxy,
                keep_tmp_files=keep_tmp_files,
                ignore_position_id=ignore_position_id,
            )
        case DataSource.big_query:
            if "big_query" not in conf_file["from"]:
                raise RuntimeError("should have [from.big_query]")
            start_time = datetime.strptime(conf_file["from"]["big_query"]["start"], "%Y-%m-%d").date()
            end_time = datetime.strptime(conf_file["from"]["big_query"]["end"], "%Y-%m-%d").date()
            auth_file = conf_file["from"]["big_query"]["auth_file"]
            http_proxy = None
            if "http_proxy" in conf_file["from"]["big_query"]:
                http_proxy = conf_file["from"]["big_query"]["http_proxy"]
            from_config.big_query = BigQueryConfig(
                start=start_time,
                end=end_time,
                auth_file=auth_file,
                http_proxy=http_proxy,
            )

    return Config(from_config, to_config)

def fill_file_names(config:Config)->Config:
    raw_file_list=[]
    if config.from_config.data_source in [DataSource.big_query, DataSource.rpc]:
        if config.from_config.data_source==DataSource.big_query:
            days= TimeUtil.get_date_array( config.from_config.big_query.start,config.from_config.big_query.end)
        elif config.from_config.data_source==DataSource.rpc:
            days= TimeUtil.get_date_array( config.from_config.rpc.start,config.from_config.rpc.end)
        if config.from_config.dapp_type==DappType.uniswap:
            raw_file_list=[get_file_name(config.from_config.chain,config)  for d in days]


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


class DataUtil(object):
    @staticmethod
    def fill_missing(data_list: List[MinuteData]) -> List[MinuteData]:
        if len(data_list) < 1:
            return data_list
        # take the first minute in data. instead of 0:00:00
        # so here will be a problem, if the first data is 0:03:00, the first 2 minutes will be blank
        # that's because there is no previous data to follow
        # those empty rows will be filled in loading stage
        index_minute = data_list[0].timestamp
        new_list = []
        data_list_index = 0

        start_day = data_list[0].timestamp.day
        while index_minute.day == start_day:
            if (data_list_index < len(data_list)) and (index_minute == data_list[data_list_index].timestamp):
                item = data_list[data_list_index]
                data_list_index += 1
            else:
                item = MinuteData()
                item.timestamp = index_minute
            prev_data = new_list[len(new_list) - 1] if len(new_list) - 1 >= 0 else None
            # if no previous(this might happen in the first minutes) data, this row will be discarded
            if item.fill_missing_field(prev_data):
                new_list.append(item)
            index_minute = index_minute + timedelta(minutes=1)

        return new_list


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



