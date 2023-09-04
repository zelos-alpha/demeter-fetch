import os
from datetime import date, timedelta
from enum import Enum
from typing import List


class BigQueryChain(Enum):
    ethereum = {"table_name": "bigquery-public-data.crypto_ethereum.logs"}
    polygon = {"table_name": "public-data-finance.crypto_polygon.logs"}


def set_environment(auth_file: str, http_proxy: str = None):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = auth_file
    if http_proxy:
        os.environ["http_proxy"] = http_proxy
        os.environ["https_proxy"] = http_proxy


def get_date_array(date_begin, date_end) -> List[date]:
    return [date_begin + timedelta(days=x) for x in range(0, 1 + (date_end - date_begin).days)]
