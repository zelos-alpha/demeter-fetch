import dataclasses
import json
import os
import sys

import toml

import _typing as types
import uniswap_downloader
import utils

if __name__ == '__main__':
    if len(sys.argv) == 1:
        utils.print_log("please set a config file. in toml format. eg: 'python main.py config.toml'.")
        exit(1)
    if not os.path.exists(sys.argv[1]):
        utils.print_log("config file not found,")
        exit(1)
    config_file = toml.load(sys.argv[1])
    try:
        config: types.Config = utils.convert_to_config(config_file)
    except RuntimeError as e:
        utils.print_log(e)
        exit(1)
    utils.print_log("Download config:", json.dumps(dataclasses.asdict(config), indent=4))

    uniswap_downloader.download(config)
