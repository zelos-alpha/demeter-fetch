import dataclasses
import json
import os
import sys
import argparse
import time
from datetime import date, datetime

import toml

import demeter_fetch as df
import demeter_fetch.uniswap_downloader as uniswap_downloader
import demeter_fetch.aave_downloader as aave_downloader
import demeter_fetch.utils as utils
from demeter_fetch import general_downloader, FromConfig, ChainType, DappType, ChifraConfig, UniswapConfig, AaveConfig
from demeter_fetch.source_chifra import chifra_utils


def download(cfg_path):
    if not os.path.exists(cfg_path):
        utils.print_log("config file not found,")
        exit(1)
    config_file = toml.load(cfg_path)
    try:
        config: df.Config = utils.convert_to_config(config_file)
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

    downloader.download(config)


def date_to_height(chifra_params):
    chain = ChainType[chifra_params.chain]
    start = datetime.strptime(chifra_params.start, "%Y-%m-%d").date()
    end = datetime.strptime(chifra_params.end, "%Y-%m-%d").date()
    start_height = utils.ApiUtil.query_blockno_from_time(
        chain,
        datetime.combine(start, datetime.min.time()),
        False,
        chifra_params.http_proxy,
        chifra_params.key,
    )
    sleep_time = 8
    if chifra_params.key is not None:
        sleep_time = 1
    utils.print_log(f"Querying end timestamp, wait for {sleep_time} seconds to prevent max rate limit")
    time.sleep(sleep_time)  # to prevent request limit
    end_height = utils.ApiUtil.query_blockno_from_time(
        chain,
        datetime.combine(end, datetime.max.time()),
        True,
        chifra_params.http_proxy,
        chifra_params.key,
    )
    print(f"Height range, start: {start_height}, end: {end_height}")


def main():
    if len(sys.argv) == 1:
        utils.print_log("use parameter -h for help")  # please set a config file. in toml format. eg: 'python main.py config.toml'.
        exit(1)
    if len(sys.argv) > 1 and sys.argv[1].endswith(".toml"):
        download(sys.argv[1])
    else:
        argParser = argparse.ArgumentParser()
        argParser.add_argument("-c", "--config", help="Path of config file")
        parser_tool_sub = argParser.add_subparsers(help="demeter-fetch tools", dest="tools")

        parser_tool_chifra = parser_tool_sub.add_parser("date_to_height", help="Query block height in certain date range")
        parser_tool_chifra.add_argument("-c", "--chain", help="chain name, [ethereum, polygon]")
        parser_tool_chifra.add_argument("-s", "--start", help="start date, eg: 2023-1-1")
        parser_tool_chifra.add_argument("-e", "--end", help="end date, eg: 2023-1-1")
        parser_tool_chifra.add_argument("-p", "--http_proxy", help="proxy, eg: https://localhost:7890, optional", default="")
        parser_tool_chifra.add_argument("-k", "--key", help="key in etherscan, optional", default="")

        args = argParser.parse_args()
        if args.config is not None:
            download(args.config)
        elif args.tools is not None:
            if args.tools == "date_to_height":
                date_to_height(args)
                # args=argParser.parse_args(["tool","chifra",])a
                # print(args)
            pass

if __name__ == "__main__":
    main()
