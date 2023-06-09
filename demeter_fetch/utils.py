import json
from datetime import datetime, timedelta

from ._typing import *


def get_file_name(chain: ChainType, pool_address, day: date, is_raw=True):
    return f"{chain.name}-{pool_address}-{day.strftime('%Y-%m-%d')}.{'raw' if is_raw else 'processed'}.csv"


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
    to_config = ToConfig(to_type, save_path, multi_process)

    chain = ChainType[conf_file["from"]["chain"]]
    data_source = DataSource[conf_file["from"]["datasource"]]
    pool_address = conf_file["from"]["pool_address"].lower()

    from_config = FromConfig(chain, data_source, pool_address)
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
        case DataSource.chifra_log:
            if "chifra_log" not in conf_file["from"]:
                raise RuntimeError("should have [from.chifra_log]")
            files = None
            if "files" in conf_file["from"]["chifra_log"]:
                files = conf_file["from"]["chifra_log"]["files"]
            folder = None
            if "folder" in conf_file["from"]["chifra_log"]:
                folder = conf_file["from"]["chifra_log"]["folder"]
            if files is None and folder is None:
                raise RuntimeError("file_path and folder can not both null")

            from_config.chifra_log = FileConfig(files, folder)
            if to_config.type not in [ToType.raw, ToType.tick]:
                raise RuntimeError("to type should be tick or raw")
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
            if "ignore_position_id" in conf_file["from"]["rpc"]:
                ignore_position_id = conf_file["from"]["rpc"]["ignore_position_id"]
            end_point = conf_file["from"]["rpc"]["end_point"]
            start_time = datetime.strptime(conf_file["from"]["rpc"]["start"], "%Y-%m-%d").date()
            end_time = datetime.strptime(conf_file["from"]["rpc"]["end"], "%Y-%m-%d").date()
            batch_size = int(conf_file["from"]["rpc"]["batch_size"])

            from_config.rpc = RpcConfig(end_point=end_point,
                                        start=start_time,
                                        end=end_time,
                                        batch_size=batch_size,
                                        auth_string=auth_string,
                                        http_proxy=http_proxy,
                                        keep_tmp_files=keep_tmp_files,
                                        ignore_position_id=ignore_position_id)
        case DataSource.big_query:
            if "big_query" not in conf_file["from"]:
                raise RuntimeError("should have [from.big_query]")
            start_time = datetime.strptime(conf_file["from"]["big_query"]["start"], "%Y-%m-%d").date()
            end_time = datetime.strptime(conf_file["from"]["big_query"]["end"], "%Y-%m-%d").date()
            auth_file = conf_file["from"]["big_query"]["auth_file"]
            http_proxy = None
            if "http_proxy" in conf_file["from"]["big_query"]:
                http_proxy = conf_file["from"]["big_query"]["http_proxy"]
            from_config.big_query = BigQueryConfig(start=start_time,
                                                   end=end_time,
                                                   auth_file=auth_file,
                                                   http_proxy=http_proxy)

    return Config(from_config, to_config)


class TextUtil(object):
    @staticmethod
    def cut_after(text: str, symbol: str) -> str:
        index = text.find(symbol)
        return text[0:index]


class TimeUtil(object):
    @staticmethod
    def get_minute(time: datetime) -> datetime:
        return datetime(time.year, time.month, time.day, time.hour, time.minute, 0)


class HexUtil(object):
    @staticmethod
    def to_signed_int(h):
        """
        Converts hex values to signed integers.
        """
        s = bytes.fromhex(h[2:])
        i = int.from_bytes(s, 'big', signed=True)
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
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)
