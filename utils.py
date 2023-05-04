from datetime import datetime

from _typing import *


def get_file_name(chain: ChainType, pool_address, day: date, is_raw=True):
    return f"{chain.name}-{pool_address}-{day.strftime('%Y-%m-%d')}.{'raw' if is_raw else 'processed'}.csv"


def convert_to_config(conf_file: dict) -> Config:
    to_type = ToType[conf_file["to"]["type"]]
    save_path = "./"
    if "save_path" in conf_file["to"]:
        save_path = conf_file["to"]["save_path"]
    tick_config = TickConfig()
    if "tick" in conf_file["to"]:
        get_position_id = conf_file["to"]["tick"]["get_position_id"]
        tick_config.get_position_id = get_position_id

    to_config = ToConfig(to_type, save_path, tick_config)

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
            proxy_file_path = None
            if "proxy_file_path" in conf_file["from"]["file"]:
                proxy_file_path = conf_file["from"]["file"]["proxy_file_path"]
            if files is None and folder is None:
                raise RuntimeError("file_path and folder can not both null")
            if to_config.tick_config.get_position_id and not proxy_file_path:
                raise RuntimeError("no proxy file")

            from_config.file = FileConfig(files, folder, proxy_file_path)
        case DataSource.rpc:
            if "rpc" not in conf_file["from"]:
                raise RuntimeError("should have [from.rpc]")
            proxy_file_path = None
            if "proxy_file_path" in conf_file["from"]["rpc"]:
                proxy_file_path = conf_file["from"]["rpc"]["proxy_file_path"]
            auth_string = None
            if "auth_string" in conf_file["from"]["rpc"]:
                auth_string = conf_file["from"]["rpc"]["auth_string"]
            http_proxy = None
            if "http_proxy" in conf_file["from"]["rpc"]:
                http_proxy = conf_file["from"]["rpc"]["http_proxy"]
            end_point = conf_file["from"]["rpc"]["end_point"]
            start_height = int(conf_file["from"]["rpc"]["start_height"])
            end_height = int(conf_file["from"]["rpc"]["end_height"])
            batch_size = int(conf_file["from"]["rpc"]["batch_size"])
            if to_config.tick_config.get_position_id and not proxy_file_path:
                raise RuntimeError("no proxy file")
            from_config.rpc = RpcConfig(end_point=end_point,
                                        start_height=start_height,
                                        end_height=end_height,
                                        batch_size=batch_size,
                                        auth_string=auth_string,
                                        http_proxy=http_proxy,
                                        proxy_file_path=proxy_file_path)
        case DataSource.big_query:
            if "big_query" not in conf_file["from"]:
                raise RuntimeError("should have [from.big_query]")
            start_time = datetime.strptime(conf_file["from"]["big_query"]["start"], "%Y-%m-%d").date()
            end_time = datetime.strptime(conf_file["from"]["big_query"]["end"], "%Y-%m-%d").date()
            auth_file = conf_file["from"]["big_query"]["auth_file"]
            http_proxy = None
            if "http_proxy" in conf_file["from"]["big_query"]:
                http_proxy = conf_file["from"]["big_query"]["http_proxy"]
            from_config.big_query = BigQueryConfig(start_time, end_time, auth_file, http_proxy)

    return Config(from_config, to_config)
