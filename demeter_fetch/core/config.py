#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-08 16:37
# @Author  : 32ethers
# @Description:

from demeter_fetch.common import *


def get_item_with_default_2(cfg: Dict, key1: str, key2: str, default_val):
    val = default_val
    if key2 in cfg[key1]:
        val = cfg[key1][key2]
    return val


def get_item_with_default_3(cfg: Dict, key1: str, key2: str, key3: str, default_val, converter=None):
    val = default_val
    if key3 in cfg[key1][key2]:
        val = cfg[key1][key2][key3]
    if converter is None:
        return val
    else:
        return converter(val)


def convert_to_config(conf_file: dict) -> Config:
    to_type = ToType[conf_file["to"]["type"]]
    save_path = get_item_with_default_2(conf_file, "to", "save_path", "../")
    multi_process = get_item_with_default_2(conf_file, "to", "multi_process", False)
    skip_existed = get_item_with_default_2(conf_file, "to", "skip_existed", False)
    to_config = ToConfig(to_type, save_path, multi_process, skip_existed)

    chain = ChainType[conf_file["from"]["chain"]]
    data_source = DataSource[conf_file["from"]["datasource"]]

    dapp_type = DappType[conf_file["from"]["dapp_type"]]

    http_proxy = get_item_with_default_2(conf_file, "from", "http_proxy", None)
    from_config = FromConfig(chain=chain, data_source=data_source, dapp_type=dapp_type, http_proxy=http_proxy)

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
            files = get_item_with_default_3(conf_file, "from", "file", "files", None)
            folder = get_item_with_default_3(conf_file, "from", "file", "folder", None)

            if files is None and folder is None:
                raise RuntimeError("file_path and folder can not both null")

            from_config.file = FileConfig(files, folder)

        case DataSource.rpc:
            if "rpc" not in conf_file["from"]:
                raise RuntimeError("should have [from.rpc]")
            auth_string = get_item_with_default_3(conf_file, "from", "rpc", "auth_string", None)
            keep_tmp_files = get_item_with_default_3(conf_file, "from", "rpc", "keep_tmp_files", None)
            ignore_position_id = get_item_with_default_3(conf_file, "from", "rpc", "ignore_position_id", False)
            etherscan_api_key = get_item_with_default_3(conf_file, "from", "rpc", "etherscan_api_key", None)
            end_point = conf_file["from"]["rpc"]["end_point"]
            start_time = datetime.strptime(conf_file["from"]["rpc"]["start"], "%Y-%m-%d").date()
            end_time = datetime.strptime(conf_file["from"]["rpc"]["end"], "%Y-%m-%d").date()
            batch_size = get_item_with_default_3(conf_file, "from", "rpc", "batch_size", 500)

            from_config.rpc = RpcConfig(
                end_point=end_point,
                start=start_time,
                end=end_time,
                batch_size=batch_size,
                auth_string=auth_string,
                keep_tmp_files=keep_tmp_files,
                ignore_position_id=ignore_position_id,
                etherscan_api_key=etherscan_api_key,
            )
        case DataSource.big_query:
            if "big_query" not in conf_file["from"]:
                raise RuntimeError("should have [from.big_query]")
            start_time = datetime.strptime(conf_file["from"]["big_query"]["start"], "%Y-%m-%d").date()
            end_time = datetime.strptime(conf_file["from"]["big_query"]["end"], "%Y-%m-%d").date()
            auth_file = conf_file["from"]["big_query"]["auth_file"]
            from_config.big_query = BigQueryConfig(
                start=start_time,
                end=end_time,
                auth_file=auth_file,
            )
        case DataSource.chifra:
            if "chifra" not in conf_file["from"]:
                raise RuntimeError("should have [from.chifra]")

            start_time = get_item_with_default_3(conf_file, "from", "chifra", "start", None, lambda x: datetime.strptime(x, "%Y-%m-%d").date())
            end_time = get_item_with_default_3(conf_file, "from", "chifra", "end_time", None, lambda x: datetime.strptime(x, "%Y-%m-%d").date())

            file_path = conf_file["from"]["chifra"]["file_path"]
            ignore_position_id = get_item_with_default_3(conf_file, "from", "chifra", "ignore_position_id", False)
            proxy_file_path = get_item_with_default_3(conf_file, "from", "chifra", "proxy_file_path", None)

            if not ignore_position_id and proxy_file_path is None:
                raise RuntimeError("If you want to append uniswap proxy logs, you should export proxy pool, too")

            etherscan_api_key = get_item_with_default_3(conf_file, "from", "chifra", "etherscan_api_key", None)

            from_config.chifra_config = ChifraConfig(
                file_path=file_path,
                ignore_position_id=ignore_position_id,
                proxy_file_path=proxy_file_path,
                etherscan_api_key=etherscan_api_key,
                start=start_time,
                end=end_time,
            )

    return Config(from_config, to_config)
