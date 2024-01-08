#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-08 16:21
# @Author  : 32ethers
# @Description:

import dataclasses
import json
import os

import toml

import demeter_fetch as df
import demeter_fetch.aave_downloader as aave_downloader
import demeter_fetch.uniswap_downloader as uniswap_downloader
import demeter_fetch.common as utils
from demeter_fetch import general_downloader
from .config import convert_to_config

def download(cfg_path):
    if not os.path.exists(cfg_path):
        utils.print_log("config file not found,")
        exit(1)
    config_file = toml.load(cfg_path)
    try:
        config: df.Config = convert_to_config(config_file)
    except RuntimeError as e:
        utils.print_log(e)
        exit(1)
    utils.print_log(
        "Download config:",
        json.dumps(dataclasses.asdict(config), cls=utils.ComplexEncoder, indent=4),
    )
    downloader = general_downloader.GeneralDownloader()
    match config.from_config.dapp_type:
        case df.DappType.uniswap:
            downloader = uniswap_downloader.Downloader()
        case df.DappType.aave:
            downloader = aave_downloader.Downloader()

    config = downloader.set_file_list(config)

    # downloader.download(config)
