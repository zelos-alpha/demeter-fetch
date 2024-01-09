#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-08 16:21
# @Author  : 32ethers
# @Description:

import dataclasses
import json
import os
from datetime import timedelta

import toml

import demeter_fetch as df
import demeter_fetch.aave_downloader as aave_downloader
import demeter_fetch.uniswap_downloader as uniswap_downloader
import demeter_fetch.common as utils
from demeter_fetch import general_downloader, DataSource
from . import engine
from .config import convert_to_config
from tqdm import tqdm

from .nodes import get_root_node


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

    steps = engine.generate_tree(get_root_node(config.from_config.dapp_type, config.to_config.type))

    for step in steps:
        if step.is_download:
            # update download according to source types
            match config.from_config.data_source:
                case DataSource.big_query:
                    pass
                case DataSource.rpc:
                    pass
                case DataSource.file:
                    pass
                case DataSource.chifra:
                    pass
                case _:
                    raise NotImplemented("Unknown datasource " + config.from_config.data_source)

            pass

    if config.from_config.start is not None and config.from_config.end is not None:
        day_idx = config.from_config.start
        with tqdm(total=(config.from_config.end - config.from_config.start).days, ncols=120) as pbar:
            while day_idx <= config.from_config.end:
                output = {}
                for step in steps:
                    param = {n.name: output[n.name] for n in step.depend}
                    step_output = step.processor(config, param)
                    output[step.name] = step_output
                day_idx += timedelta(days=1)
                pbar.update()
    else:
        pass

    # downloader = general_downloader.GeneralDownloader()
    # match config.from_config.dapp_type:
    #     case df.DappType.uniswap:
    #         downloader = uniswap_downloader.Downloader()
    #     case df.DappType.aave:
    #         downloader = aave_downloader.Downloader()

    # config = downloader.set_file_list(config)

    # downloader.download(config)
