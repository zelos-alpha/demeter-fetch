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
from tqdm import tqdm

import demeter_fetch as df
import demeter_fetch.common as utils
from . import engine
from .config import convert_to_config
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

    root_node = get_root_node(config.from_config.dapp_type, config.to_config.type)
    steps = engine.generate_tree(root_node)

    if config.from_config.start is not None and config.from_config.end is not None:
        day_idx = config.from_config.start
        with tqdm(total=(config.from_config.end - config.from_config.start).days, ncols=120) as pbar:
            while day_idx <= config.from_config.end:
                output = {}
                for step in steps:
                    if config.to_config.skip_existed and os.path.exists(os.path.join(config.to_config.save_path, step.file_name(config.from_config))):
                        continue
                    param = {n.name: output[n.name] for n in step.depend}
                    step_output = step.processor(config, day_idx, param)
                    output[step.name] = step_output
                    if config.to_config.keep_raw or step == root_node:
                        step_output.df.to_csv(
                            os.path.join(config.to_config.save_path, step.file_name(config.from_config, day_idx.strftime("%Y-%m-%d"))), index=False
                        )
                day_idx += timedelta(days=1)
                pbar.update()
                # print(day_idx, output)
    else:
        pass
