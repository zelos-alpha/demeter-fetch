import os
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from enum import Enum
from google.cloud import bigquery
from google.cloud.bigquery import Client
from typing import Tuple

from demeter_fetch import ChainType

global_client: Client | None = None


def _set_environment(auth_file: str, http_proxy: str = None):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = auth_file
    if http_proxy:
        os.environ["http_proxy"] = http_proxy
        os.environ["https_proxy"] = http_proxy


class SupportedChain(Enum):
    arbitrum = "bigquery-public-data.goog_blockchain_arbitrum_one_us.blocks"
    optimism = "bigquery-public-data.goog_blockchain_optimism_mainnet_us.blocks"
    polygon = "bigquery-public-data.goog_blockchain_polygon_mainnet_us.blocks"
    ethereum = "bigquery-public-data.goog_blockchain_ethereum_mainnet_us.blocks"
    avalanche = "bigquery-public-data.goog_blockchain_avalanche_contract_chain_us.blocks"


class HeightCacheGenerator:
    def save(self, results):
        pass


class HeightCacheGeneratorLevelDB(HeightCacheGenerator):
    """
    height => block_timestamp cache
    """

    height_cache_file_name = "_height_timestamp_levelDB"

    def __init__(self, chain: ChainType, save_path: str):
        import plyvel

        self.height_cache_path = os.path.join(
            save_path, chain.name + HeightCacheGeneratorLevelDB.height_cache_file_name
        )
        self._block_dict = plyvel.DB(self.height_cache_path, create_if_missing=True)

    def save(self, results):
        for page in results.pages:
            with self._block_dict.write_batch() as wb:
                for row in page:
                    wb.put(row[0].to_bytes(4), int(row[1].timestamp() * 1000).to_bytes(6))

    def __del__(self):
        self._block_dict.close()


class HeightCacheGeneratorSqlite(HeightCacheGenerator):
    """
    height => block_timestamp cache
    """

    height_cache_file_name = "_height_timestamp.sqlite"

    def __init__(self, chain: ChainType, save_path: str):
        from sqlitedict import SqliteDict

        self.height_cache_path = os.path.join(save_path, chain.name + HeightCacheGeneratorSqlite.height_cache_file_name)
        self._block_dict = SqliteDict(self.height_cache_path, outer_stack=False)

    def save(self, results):
        in_mem_count = 0
        for page in results.pages:
            for row in page:
                self._block_dict[row[0]] = row[1]
                in_mem_count += 1
                if in_mem_count >= 1000:
                    self._block_dict.commit()
                    in_mem_count = 0
        self._block_dict.commit()

    def __del__(self):
        self._block_dict.close()


def _download(block_table: str, year: int, month: int, auth_path: str, proxy: str, days: Tuple[int, int] = None):
    if days is None:
        sql = (
            f"SELECT block_number,block_timestamp FROM `{block_table}` "
            f'WHERE TIMESTAMP_TRUNC(block_timestamp, MONTH) = TIMESTAMP("{year}-{month}-01")'
        )
    else:
        sql = (
            f"SELECT block_number,block_timestamp FROM `{block_table}` "
            f'WHERE DATE(block_timestamp) >= DATE("{year}-{month}-{days[0]}") '
            f'AND DATE(block_timestamp) <= DATE("{year}-{month}-{days[1]}");'
        )

    _set_environment(auth_path, proxy)
    global global_client
    if global_client is None:
        global_client = bigquery.Client()
    query_job = global_client.query(sql)  # Make an API request.
    result = query_job.result(page_size=1_000_000)
    return result


def download_between(
    start_date: date,
    end_date: date,
    chain: ChainType,
    google_auth_path: str,
    proxy: str,
    manager: HeightCacheGenerator,
):
    current_month = date(start_date.year, start_date.month, 1)

    while current_month <= end_date:
        print(datetime.now(), f"Querying {current_month.strftime('%Y-%m')}")
        big_query_iter = _download(
            SupportedChain[chain.name].value,
            current_month.year,
            current_month.month,
            google_auth_path,
            proxy,
        )
        print(datetime.now(), f"saving {current_month.strftime('%Y-%m')}")
        manager.save(big_query_iter)

        current_month = current_month + relativedelta(months=1)


def get_block_and_timestamp_cache(
    google_auth_path: str, chain: ChainType, to_path: str, start: date, end: date, proxy: str, engine: str
):
    print("===============preparing height cache from big query===============")

    if not os.path.exists(to_path):
        os.mkdir(to_path)
    if engine == "sqlite":
        mgr = HeightCacheGeneratorSqlite(chain, to_path)
    elif engine == "levelDB":
        mgr = HeightCacheGeneratorLevelDB(chain, to_path)
    else:
        raise RuntimeError(f"Unknown engine {engine}")
    download_between(start, end, chain, google_auth_path, proxy, mgr)
    print(datetime.now(), "saved to " + mgr.height_cache_path)
