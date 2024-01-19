import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import List

import demeter_fetch.common.utils as utils
from .. import ChainType


@dataclass
class ContractConfig:
    address: str
    topics: List[str]


def get_height_from_date(
    day: date, chain: ChainType, http_proxy, etherscan_api_key, sleep_seconds=1, sleep_seconds_without_key=8
) -> (int, int):
    utils.print_log(f"Query height range in {day}")
    if etherscan_api_key is None:
        sleep_seconds = sleep_seconds_without_key
    start_height = utils.ApiUtil.query_blockno_from_time(
        chain, datetime.combine(day, datetime.min.time()), False, http_proxy, etherscan_api_key
    )
    utils.print_log(f"Querying end timestamp, wait for {sleep_seconds} seconds to prevent max rate limit")
    time.sleep(sleep_seconds)  # to prevent request limit
    end_height = utils.ApiUtil.query_blockno_from_time(
        chain, datetime.combine(day, datetime.max.time()), True, http_proxy, etherscan_api_key
    )

    return start_height, end_height
