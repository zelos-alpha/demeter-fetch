import time
from datetime import datetime

import demeter_fetch.common.utils as utils
from demeter_fetch import ChainType
from demeter_fetch.tools import bigquery_tools


def date_to_height(args):
    chain = ChainType[args.chain]
    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()
    start_height = utils.ApiUtil.query_blockno_from_time(
        chain,
        datetime.combine(start, datetime.min.time()),
        False,
        args.http_proxy,
        args.key,
    )
    sleep_time = 8
    if args.key is not None and args.key != "":
        sleep_time = 0.5
    utils.print_log(f"Querying end timestamp, wait for {sleep_time} seconds to prevent max rate limit")
    time.sleep(sleep_time)  # to prevent request limit
    end_height = utils.ApiUtil.query_blockno_from_time(
        chain,
        datetime.combine(end, datetime.max.time()),
        True,
        args.http_proxy,
        args.key,
    )
    print(f"Height range, start: {start_height}, end: {end_height}")


def block_timestamp_cache(args):
    chain = ChainType[args.chain]
    start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end, "%Y-%m-%d").date()
    to_path = args.to
    proxy = args.http_proxy
    auth_path = args.key
    engine = args.engine
    bigquery_tools.get_block_and_timestamp_cache(auth_path, chain, to_path, start_date, end_date, proxy, engine)
