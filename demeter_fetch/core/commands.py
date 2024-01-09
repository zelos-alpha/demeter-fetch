import argparse


def get_commend_args():
    argParser = argparse.ArgumentParser()
    argParser.add_argument("-c", "--config", help="Path of config file")
    parser_tool_sub = argParser.add_subparsers(help="demeter-fetch tools", dest="tools")

    parser_tool_chifra = parser_tool_sub.add_parser("date_to_height", help="Query block height from etherscan in certain date range")
    parser_tool_chifra.add_argument("-c", "--chain", help="chain name, [ethereum, polygon]")
    parser_tool_chifra.add_argument("-s", "--start", help="start date, eg: 2023-1-1")
    parser_tool_chifra.add_argument("-e", "--end", help="end date, eg: 2023-1-1")
    parser_tool_chifra.add_argument("-p", "--http_proxy", help="proxy, eg: https://localhost:7890, optional", default="")
    parser_tool_chifra.add_argument("-k", "--key", help="key in etherscan, optional", default="")

    args = argParser.parse_args()
    return args
