#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-08 16:37
# @Author  : 32ethers
# @Description:
from typing import Dict

from demeter_fetch.common import *


def get_item_with_default(cfg: Dict, keys: List, default_val, converter=None):
    def return_val():
        if converter is None or val is None:
            return val
        else:
            return converter(val)

    val = default_val
    obj = cfg
    for key in keys:
        if key in obj:
            obj = obj[key]
        else:
            return return_val()
    val = obj
    return return_val()


def get_item_with_default_2(cfg: Dict, key1: str, key2: str, default_val, converter=None):
    return get_item_with_default(cfg, [key1, key2], default_val, converter)
    # val = default_val
    # if key2 in cfg[key1]:
    #     val = cfg[key1][key2]
    # if converter is None or val is None:
    #     return val
    # else:
    #     return converter(val)


def get_item_with_default_3(cfg: Dict, key1: str, key2: str, key3: str, default_val, converter=None):
    return get_item_with_default(cfg, [key1, key2, key3], default_val, converter)


def get_item_with_default_4(cfg: Dict, key1: str, key2: str, key3: str, key4: str, default_val, converter=None):
    return get_item_with_default(cfg, [key1, key2, key3, key4], default_val, converter)


def convert_to_config(conf_file: dict) -> Config:
    to_type = ToType[conf_file["to"]["type"]]
    save_path = get_item_with_default_2(conf_file, "to", "save_path", "../")
    multi_process = get_item_with_default_2(conf_file, "to", "multi_process", False)
    skip_existed = get_item_with_default_2(conf_file, "to", "skip_existed", False)
    keep_raw = get_item_with_default_2(conf_file, "to", "keep_raw", False)
    to_file_type = get_item_with_default_2(conf_file, "to", "file_type", ToFileType.csv, lambda x: ToFileType[x])
    to_config = ToConfig(to_type, save_path, multi_process, skip_existed, keep_raw, to_file_type)

    chain = ChainType[conf_file["from"]["chain"]]
    data_source = DataSource[conf_file["from"]["datasource"]]
    dapp_type = DappType[conf_file["from"]["dapp_type"]]
    http_proxy = get_item_with_default_2(conf_file, "from", "http_proxy", None)
    start_time = get_item_with_default_2(
        conf_file, "from", "start", None, lambda x: datetime.strptime(x, "%Y-%m-%d").date()
    )
    end_time = get_item_with_default_2(
        conf_file, "from", "end", None, lambda x: datetime.strptime(x, "%Y-%m-%d").date()
    )
    from_config = FromConfig(
        chain=chain, data_source=data_source, dapp_type=dapp_type, http_proxy=http_proxy, start=start_time, end=end_time
    )

    if start_time is None or end_time is None:
        raise RuntimeError("start time and end time must be set")
    if dapp_type in [DappType.uniswap, DappType.uniswap_v4]:
        pool_address = conf_file["from"]["uniswap"]["pool_address"].lower()
        ignore_position_id = get_item_with_default_3(conf_file, "from", "uniswap", "ignore_position_id", False)
        from_config.uniswap_config = UniswapConfig(pool_address, ignore_position_id)
        if "token0" in conf_file["from"]["uniswap"]:
            token0_name = get_item_with_default_4(conf_file, "from", "uniswap", "token0", "name", "")
            token0_decimal = get_item_with_default_4(conf_file, "from", "uniswap", "token0", "decimal", 0)
            from_config.uniswap_config.token0 = TokenConfig(token0_name, token0_decimal)
        if "token1" in conf_file["from"]["uniswap"]:
            token1_name = get_item_with_default_4(conf_file, "from", "uniswap", "token1", "name", "")
            token1_decimal = get_item_with_default_4(conf_file, "from", "uniswap", "token1", "decimal", 0)
            from_config.uniswap_config.token1 = TokenConfig(token1_name, token1_decimal)
        from_config.uniswap_config.is_token0_base = get_item_with_default_3(
            conf_file, "from", "uniswap", "is_token0_base", None
        )

    elif dapp_type == DappType.aave:
        token_addresses = [x.lower() for x in conf_file["from"]["aave"]["tokens"]]
        from_config.aave_config = AaveConfig(token_addresses)
    elif dapp_type == DappType.gmx_v2:
        pool_address = conf_file["from"]["gmx_v2"]["gm_address"].lower()
        long_token = TokenConfig(
            conf_file["from"]["gmx_v2"]["long_token"]["name"].lower(),
            conf_file["from"]["gmx_v2"]["long_token"]["decimal"],
            conf_file["from"]["gmx_v2"]["long_token"]["address"].lower(),
        )
        short_token = TokenConfig(
            conf_file["from"]["gmx_v2"]["short_token"]["name"].lower(),
            conf_file["from"]["gmx_v2"]["short_token"]["decimal"],
            conf_file["from"]["gmx_v2"]["short_token"]["address"].lower(),
        )
        index_token = TokenConfig(
            conf_file["from"]["gmx_v2"]["index_token"]["name"].lower(),
            conf_file["from"]["gmx_v2"]["index_token"]["decimal"],
            conf_file["from"]["gmx_v2"]["index_token"]["address"].lower(),
        )
        from_config.gmx_v2_config = GmxV2Config(pool_address, long_token, short_token, index_token)

    if data_source not in ChainTypeConfig[from_config.chain]["allow"]:
        raise RuntimeError(f"{data_source.name} is not allowed to download from {from_config.chain.name}")
    match data_source:
        case DataSource.rpc:
            if "rpc" not in conf_file["from"]:
                raise RuntimeError("should have [from.rpc]")
            auth_string = get_item_with_default_3(conf_file, "from", "rpc", "auth_string", None)
            keep_tmp_files = get_item_with_default_3(conf_file, "from", "rpc", "keep_tmp_files", None)
            if keep_raw == False and keep_tmp_files is None:
                keep_tmp_files = False
            etherscan_api_key = get_item_with_default_3(conf_file, "from", "rpc", "etherscan_api_key", None)
            end_point = conf_file["from"]["rpc"]["end_point"]
            batch_size = get_item_with_default_3(conf_file, "from", "rpc", "batch_size", 500)
            force_no_proxy = get_item_with_default_3(conf_file, "from", "rpc", "force_no_proxy", False)
            height_cache_path = get_item_with_default_3(conf_file, "from", "rpc", "height_cache_path", None)
            thread = get_item_with_default_3(conf_file, "from", "rpc", "thread", None)

            from_config.rpc = RpcConfig(
                end_point=end_point,
                batch_size=batch_size,
                auth_string=auth_string,
                keep_tmp_files=keep_tmp_files,
                etherscan_api_key=etherscan_api_key,
                force_no_proxy=force_no_proxy,
                height_cache_path=height_cache_path,
                thread=thread,
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
            etherscan_api_key = get_item_with_default_3(conf_file, "from", "chifra", "etherscan_api_key", None)
            from_config.chifra_config = ChifraConfig(
                etherscan_api_key=etherscan_api_key,
            )

    return Config(from_config, to_config)
