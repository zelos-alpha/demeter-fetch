#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-08 16:37
# @Author  : 32ethers
# @Description:

from demeter_fetch.common import *


def get_item_with_default_2(cfg: Dict, key1: str, key2: str, default_val, converter=None):
    val = default_val
    if key2 in cfg[key1]:
        val = cfg[key1][key2]
    if converter is None or val is None:
        return val
    else:
        return converter(val)


def get_item_with_default_3(cfg: Dict, key1: str, key2: str, key3: str, default_val, converter=None):
    val = default_val
    if key3 in cfg[key1][key2]:
        val = cfg[key1][key2][key3]
    if converter is None or val is None:
        return val
    else:
        return converter(val)


def convert_to_config(conf_file: dict) -> Config:
    to_type = ToType[conf_file["to"]["type"]]
    save_path = get_item_with_default_2(conf_file, "to", "save_path", "../")
    multi_process = get_item_with_default_2(conf_file, "to", "multi_process", False)
    skip_existed = get_item_with_default_2(conf_file, "to", "skip_existed", False)
    keep_raw = get_item_with_default_2(conf_file, "to", "keep_raw", False)
    to_config = ToConfig(to_type, save_path, multi_process, skip_existed, keep_raw)

    chain = ChainType[conf_file["from"]["chain"]]
    data_source = DataSource[conf_file["from"]["datasource"]]
    dapp_type = DappType[conf_file["from"]["dapp_type"]]
    http_proxy = get_item_with_default_2(conf_file, "from", "http_proxy", None)
    start_time = get_item_with_default_2(conf_file, "from", "start", None, lambda x: datetime.strptime(x, "%Y-%m-%d").date())
    end_time = get_item_with_default_2(conf_file, "from", "end", None, lambda x: datetime.strptime(x, "%Y-%m-%d").date())
    from_config = FromConfig(chain=chain, data_source=data_source, dapp_type=dapp_type, http_proxy=http_proxy, start=start_time, end=end_time)

    if dapp_type == DappType.uniswap:
        pool_address = conf_file["from"]["uniswap"]["pool_address"].lower()
        ignore_position_id = get_item_with_default_3(conf_file, "from", "uniswap", "ignore_position_id", False)
        from_config.uniswap_config = UniswapConfig(pool_address, ignore_position_id)

    elif dapp_type == DappType.aave:
        token_addresses = [x.lower() for x in conf_file["from"]["aave"]["tokens"]]
        from_config.aave_config = AaveConfig(token_addresses)

    if data_source not in ChainTypeConfig[from_config.chain]["allow"]:
        raise RuntimeError(f"{data_source.name} is not allowed to download from {from_config.chain.name}")
    match data_source:
        case DataSource.rpc:
            if "rpc" not in conf_file["from"]:
                raise RuntimeError("should have [from.rpc]")
            auth_string = get_item_with_default_3(conf_file, "from", "rpc", "auth_string", None)
            keep_tmp_files = get_item_with_default_3(conf_file, "from", "rpc", "keep_tmp_files", None)
            etherscan_api_key = get_item_with_default_3(conf_file, "from", "rpc", "etherscan_api_key", None)
            end_point = conf_file["from"]["rpc"]["end_point"]
            batch_size = get_item_with_default_3(conf_file, "from", "rpc", "batch_size", 500)

            from_config.rpc = RpcConfig(
                end_point=end_point,
                batch_size=batch_size,
                auth_string=auth_string,
                keep_tmp_files=keep_tmp_files,
                etherscan_api_key=etherscan_api_key,
            )
        case DataSource.big_query:
            if "big_query" not in conf_file["from"]:
                raise RuntimeError("should have [from.big_query]")
            auth_file = conf_file["from"]["big_query"]["auth_file"]
            from_config.big_query = BigQueryConfig(
                auth_file=auth_file,
            )
        case DataSource.chifra:
            if "chifra" not in conf_file["from"]:
                raise RuntimeError("should have [from.chifra]")

            file_path = conf_file["from"]["chifra"]["file_path"]
            proxy_file_path = get_item_with_default_3(conf_file, "from", "chifra", "proxy_file_path", None)

            etherscan_api_key = get_item_with_default_3(conf_file, "from", "chifra", "etherscan_api_key", None)

            from_config.chifra_config = ChifraConfig(
                file_path=file_path,
                proxy_file_path=proxy_file_path,
                etherscan_api_key=etherscan_api_key,
            )

    return Config(from_config, to_config)
