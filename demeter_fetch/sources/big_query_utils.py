import os
from datetime import date, timedelta
from enum import Enum
from typing import List
from google.cloud import bigquery
from google.cloud.bigquery import Client

global_client: Client | None = None


class BigQueryChain(Enum):
    ethereum = {"table_name": "bigquery-public-data.crypto_ethereum.logs", "uni_proxy_addr": "0xc36442b4a4522e871399cd717abdd847ab11fe88"}
    polygon = {"table_name": "public-data-finance.crypto_polygon.logs", "uni_proxy_addr": "0xc36442b4a4522e871399cd717abdd847ab11fe88"}


def _set_environment(auth_file: str, http_proxy: str = None):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = auth_file
    if http_proxy:
        os.environ["http_proxy"] = http_proxy
        os.environ["https_proxy"] = http_proxy


def get_date_array(date_begin, date_end) -> List[date]:
    return [date_begin + timedelta(days=x) for x in range(0, 1 + (date_end - date_begin).days)]


def query_by_sql(query: str, auth_file: str, http_proxy: str | None = None):
    _set_environment(auth_file, http_proxy)
    global global_client
    if global_client is None:
        global_client = bigquery.Client()
    query_job = global_client.query(query)  # Make an API request.
    result = query_job.to_dataframe(create_bqstorage_client=False)
    return result


def close_client():
    global global_client
    if global_client is not None:
        global_client.close()
