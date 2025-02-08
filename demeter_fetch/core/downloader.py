#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-08 16:21
# @Author  : 32ethers
# @Description:

import dataclasses
import json
import os
from typing import List

import toml

import demeter_fetch.common as utils
from . import engine
from .config import convert_to_config
from .. import Config
from ..common import print_log, set_global_pbar, Node


def download_by_config(config: Config) -> List[str]:
    ignore_pos = False
    if config.from_config.uniswap_config is not None:
        ignore_pos = config.from_config.uniswap_config.ignore_position_id
    if not os.path.exists(config.to_config.save_path):
        print_log(f"Creating path {config.to_config.save_path}")
        os.makedirs(config.to_config.save_path)
    root_step = engine.get_root_node(config.from_config.dapp_type, config.to_config.type, ignore_pos)
    root_step.set_config(config)
    steps: List[Node] = engine.get_relative_nodes(root_step)
    utils.print_log("Will execute the following steps: " + str(steps))
    # [step.set_config(config) for step in steps]

    for step in steps:
        set_global_pbar(None)
        print_log(f"Current step: {step.name}")
        step.work()
    if config.to_config.keep_raw:
        generated_files = []
        for step in steps:
            generated_files.extend(list(step.get_file_paths.values()))
        return generated_files
    else:
        # remove raw files
        for step in steps:
            if step != root_step:
                for param, sf in step.get_file_paths.items():
                    os.remove(sf)
        return list(root_step.get_file_paths.values())


def download(cfg_path):
    if not os.path.exists(cfg_path):
        utils.print_log("config file not found,")
        exit(1)
    config_file = toml.load(cfg_path)
    try:
        config = convert_to_config(config_file)
    except RuntimeError as e:
        utils.print_log(e)
        exit(1)
    utils.print_log(
        "Download config:",
        json.dumps(dataclasses.asdict(config), cls=utils.ComplexEncoder, indent=4),
    )

    download_by_config(config)
