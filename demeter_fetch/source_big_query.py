import os
from datetime import timedelta, date
from enum import Enum
from typing import List

import pandas as pd
from google.cloud import bigquery
from tqdm import tqdm

import demeter_fetch._typing as _typing
import demeter_fetch.constants as constants
import demeter_fetch.utils as utils


class BigQueryChain(Enum):
    ethereum = {"table_name": "bigquery-public-data.crypto_ethereum.logs"}
    polygon = {"table_name": "public-data-finance.crypto_polygon.logs"}


def download_event(chain: _typing.ChainType,
                   contract_address: str,
                   date_begin: date,
                   date_end: date,
                   data_save_path: os.path,
                   auth_file: str,
                   http_proxy: str | None = None) -> List[str]:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = auth_file
    if http_proxy:
        os.environ['http_proxy'] = http_proxy
        os.environ['https_proxy'] = http_proxy
    bq_chain = BigQueryChain[chain.name]
    date_generated = [date_begin + timedelta(days=x) for x in range(0, 1 + (date_end - date_begin).days)]
    file_names = []
    with tqdm(total=len(date_generated), ncols=150) as pbar:
        for one_day in date_generated:
            df = download_event_one_day(bq_chain, contract_address, one_day)
            file_name = os.path.join(data_save_path, utils.get_file_name(chain, contract_address, one_day))
            df.to_csv(file_name, header=True, index=False)
            file_names.append(file_name)
            pbar.update()

    return file_names


def download_event_one_day(chain: BigQueryChain, contract_address, one_date) -> pd.DataFrame:
    client = bigquery.Client()
    query = f"""
SELECT block_number, transaction_hash, block_timestamp, transaction_index  as pool_tx_index, log_index pool_log_index, topics as pool_topics, DATA as pool_data, [] as proxy_topics, '' as proxy_data,null as proxy_log_index
        FROM {chain.value["table_name"]}
        WHERE  topics[SAFE_OFFSET(0)] in ('{constants.SWAP_KECCAK}')
            AND DATE(block_timestamp) >=  DATE("{one_date}") AND DATE(block_timestamp) <=  DATE("{one_date}") AND address = "{contract_address}"

union all

select pool.block_number, pool.transaction_hash, pool.block_timestamp, pool.transaction_index as pool_tx_index, pool.log_index as pool_log_index,pool.topics as pool_topics, pool.DATA as pool_data,  proxy.topics as proxy_topics, proxy.DATA as proxy_data, proxy.log_index as proxy_log_index from
(SELECT block_number, transaction_hash, block_timestamp, transaction_index, log_index, topics, DATA FROM {chain.value["table_name"]}
        WHERE topics[SAFE_OFFSET(0)] in ('{constants.MINT_KECCAK}')
            AND DATE(block_timestamp) >=  DATE("{one_date}") AND DATE(block_timestamp) <=  DATE("{one_date}") AND address = "{contract_address}") as pool
left join 
(SELECT transaction_hash, topics, DATA,log_index FROM  {chain.value["table_name"]}
        WHERE topics[SAFE_OFFSET(0)] in ('{constants.INCREASE_LIQUIDITY}')
            AND DATE(block_timestamp) >=  DATE("{one_date}") AND DATE(block_timestamp) <=  DATE("{one_date}") AND address = "{constants.PROXY_CONTRACT_ADDRESS}") as proxy
on pool.transaction_hash=proxy.transaction_hash 

union all

select pool.block_number, pool.transaction_hash, pool.block_timestamp, pool.transaction_index as pool_tx_index, pool.log_index as pool_log_index,pool.topics as pool_topics, pool.DATA as pool_data,  proxy.topics as proxy_topics, proxy.DATA as proxy_data, proxy.log_index as proxy_log_index from
(SELECT block_number, transaction_hash, block_timestamp, transaction_index, log_index, topics, DATA FROM {chain.value["table_name"]}
        WHERE topics[SAFE_OFFSET(0)] in ('{constants.COLLECT_KECCAK}')
            AND DATE(block_timestamp) >=  DATE("{one_date}") AND DATE(block_timestamp) <=  DATE("{one_date}") AND address = "{contract_address}") as pool
left join 
(SELECT transaction_hash, topics, DATA,log_index FROM  {chain.value["table_name"]}
        WHERE topics[SAFE_OFFSET(0)] in ('{constants.COLLECT}')
            AND DATE(block_timestamp) >=  DATE("{one_date}") AND DATE(block_timestamp) <=  DATE("{one_date}") AND address = "{constants.PROXY_CONTRACT_ADDRESS}") as proxy
on pool.transaction_hash=proxy.transaction_hash 

union all

select pool.block_number, pool.transaction_hash, pool.block_timestamp, pool.transaction_index as pool_tx_index, pool.log_index as pool_log_index,pool.topics as pool_topics, pool.DATA as pool_data,  proxy.topics as proxy_topics, proxy.DATA as proxy_data, proxy.log_index as proxy_log_index from
(SELECT block_number, transaction_hash, block_timestamp, transaction_index, log_index, topics, DATA FROM  {chain.value["table_name"]}
        WHERE topics[SAFE_OFFSET(0)] in ('{constants.BURN_KECCAK}')
            AND DATE(block_timestamp) >=  DATE("{one_date}") AND DATE(block_timestamp) <=  DATE("{one_date}") AND address = "{contract_address}") as pool
left join 
(SELECT transaction_hash, topics, DATA,log_index FROM  {chain.value["table_name"]}
        WHERE topics[SAFE_OFFSET(0)] in ('{constants.DECREASE_LIQUIDITY}')
            AND DATE(block_timestamp) >=  DATE("{one_date}") AND DATE(block_timestamp) <=  DATE("{one_date}") AND address = "{constants.PROXY_CONTRACT_ADDRESS}") as proxy
on pool.transaction_hash=proxy.transaction_hash     
"""
    query_job = client.query(query)  # Make an API request.
    result = query_job.to_dataframe(create_bqstorage_client=False)
    result = result.sort_values(['block_number', 'pool_log_index'], ascending=[True, True])
    return result
