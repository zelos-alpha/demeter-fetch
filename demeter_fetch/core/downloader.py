#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-01-08 16:21
# @Author  : 32ethers
# @Description:

import dataclasses
import json
import os
from datetime import timedelta
from typing import List

import pandas as pd
import toml
from tqdm import tqdm

import demeter_fetch.common as utils
from . import engine, Node
from .config import convert_to_config
from .nodes import get_root_node
from ..common import print_log, set_global_pbar, TimeUtil


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

    ignore_pos = False
    if config.from_config.uniswap_config is not None:
        ignore_pos = config.from_config.uniswap_config.ignore_position_id

    root_node = get_root_node(config.from_config.dapp_type, config.to_config.type, ignore_pos)
    steps: List[Node] = engine.generate_tree(root_node)

    for step in steps:
        print_log(f"Current step: {step.name}")
        set_global_pbar(None)
        # if daily, global loop will handle processbar, outfile existence, gather param
        if step.is_daily:
            day_idx = config.from_config.start
            pbar = tqdm(
                total=(config.from_config.end - config.from_config.start).days + 1,
                ncols=80,
                position=0,
                leave=False,
            )
            set_global_pbar(pbar)
            while day_idx <= config.from_config.end:
                day_str = day_idx.strftime("%Y-%m-%d")
                step_file_name = step.file_name(config.from_config, day_str)
                if config.to_config.skip_existed and os.path.exists(step_file_name):
                    continue
                param = {depend.name: depend.file_name(config.from_config, day_str) for depend in step.depend}
                step.processor(config, day_idx, param, step)
                day_idx += timedelta(days=1)
                pbar.update()
        # if not daily, will handle outfile existence, gather param
        else:
            param = {}
            step_file_name = step.file_name(config.from_config, None)
            if config.to_config.skip_existed and os.path.exists(step_file_name):
                continue
            for depend in step.depend:
                param[depend.name] = [
                    depend.file_name(config.from_config, day.strftime("%Y-%m-%d"))
                    for day in TimeUtil.get_date_array(config.from_config.start, config.from_config.end)
                ]
            step.processor(config, None, param, step)
