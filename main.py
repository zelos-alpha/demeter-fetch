import dataclasses
import json
import os
import sys

import toml

import demeter_fetch as df
import demeter_fetch.uniswap_downloader as uniswap_downloader
import demeter_fetch.aave_downloader as aave_downloader
import demeter_fetch.utils as utils

if __name__ == "__main__":
    if len(sys.argv) == 1:
        utils.print_log(
            "please set a config file. in toml format. eg: 'python main.py config.toml'."
        )
        exit(1)
    if not os.path.exists(sys.argv[1]):
        utils.print_log("config file not found,")
        exit(1)
    config_file = toml.load(sys.argv[1])
    try:
        config: df.Config = utils.convert_to_config(config_file)
    except RuntimeError as e:
        utils.print_log(e)
        exit(1)
    utils.print_log(
        "Download config:",
        json.dumps(dataclasses.asdict(config), cls=utils.ComplexEncoder, indent=4),
    )

    match config.from_config.dapp_type:
        case df.DappType.uniswap:
            uniswap_downloader.download(config)
        case df.DappType.aave:
            aave_downloader.download(config)
