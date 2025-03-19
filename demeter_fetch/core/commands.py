import argparse


def get_commend_args():
    argParser = argparse.ArgumentParser()
    argParser.add_argument("-c", "--config", help="Path of config file")
    parser_tool_sub = argParser.add_subparsers(help="demeter-fetch tools", dest="tools")

    parser_height_range = parser_tool_sub.add_parser(
        "date_to_height", help="Query block height from etherscan in certain date range"
    )
    parser_height_range.add_argument("-c", "--chain", help="chain name, [ethereum, polygon]")
    parser_height_range.add_argument("-s", "--start", help="start date, eg: 2023-1-1")
    parser_height_range.add_argument("-e", "--end", help="end date, eg: 2023-1-1")
    parser_height_range.add_argument(
        "-p", "--http_proxy", help="proxy, eg: https://localhost:7890, optional", default=""
    )
    parser_height_range.add_argument("-k", "--key", help="key in etherscan, optional", default="")

    block_timestamp = parser_tool_sub.add_parser(
        "block_timestamp",
        help="Generate a cache for block number and timestamp in leveldb, datasource is bigquery, and you need install plyvel with pip first",
    )
    block_timestamp.add_argument("-c", "--chain", help="chain name, [ethereum, polygon]")
    block_timestamp.add_argument("-s", "--start", help="start date, eg: 2023-1-1")
    block_timestamp.add_argument("-e", "--end", help="end date, eg: 2023-2-1")
    block_timestamp.add_argument("-p", "--http_proxy", help="proxy, eg: https://localhost:7890, optional", default="")
    block_timestamp.add_argument("-k", "--key", help="path of google bigquery api key")
    block_timestamp.add_argument("-t", "--to", help="save cache to this path")
    block_timestamp.add_argument("-n", "--engine", help="storage type, sqlite or levelDB", default="sqlite")

    aave = parser_tool_sub.add_parser("aave", help="Get aave risk parameter")
    aave.add_argument("-c", "--chain", help="chain name, [ethereum, polygon]")
    aave.add_argument("-p", "--proxy", help="proxy, eg: https://localhost:7890, optional", default="")
    aave.add_argument("-b", "--block_number", help="block number, e.g. 10000, default is latest", default="latest")

    args = argParser.parse_args()

    return args
