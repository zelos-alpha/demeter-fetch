import time
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import List, Tuple, Dict

import demeter_fetch.common.utils as utils
from .. import ChainType


@dataclass
class ContractConfig:
    address: str
    topics0: List[str]
    topics1: List[str]=field(default_factory=list)
    topics2: List[str]=field(default_factory=list)
    topics3: List[str]=field(default_factory=list)


height_cache: Dict[date, Tuple[int, int]] = {}


def get_height_from_date(
    day: date, chain: ChainType, http_proxy, etherscan_api_key, sleep_seconds=0.5, sleep_seconds_without_key=8
) -> (int, int):
    if day in height_cache.keys():
        return height_cache[day]
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
    height_cache[day] = (start_height, end_height)
    return start_height, end_height
