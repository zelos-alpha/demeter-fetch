import os
from datetime import timedelta, date
from typing import List, Dict, Callable

import pandas as pd
from google.cloud import bigquery
from tqdm import tqdm

import demeter_fetch._typing as _typing
import demeter_fetch.constants as constants
import demeter_fetch.utils as utils
from .big_query_utils import BigQueryChain, set_environment, get_date_array


def download_event(
    chain: _typing.ChainType,
    contract_address: str,
    download_one_day_func: Callable,
    to_file_list:Dict[date,str],
    data_save_path: os.path,
    auth_file: str,
    http_proxy: str | None = None,
) -> List[str]:
    set_environment(auth_file, http_proxy)
    bq_chain_name = BigQueryChain[chain.name]
    file_names = []
    with tqdm(total=len(to_file_list), ncols=120) as pbar:
        for one_day in to_file_list.keys():
            df = download_one_day_func(bq_chain_name, contract_address, one_day)
            if contract_address == _typing.ChainTypeConfig[chain]["uniswap_proxy_addr"]:
                df["from"] = df["topics"].apply(lambda x: "0x" + x[1][26:])
                df["to"] = df["topics"].apply(lambda x: "0x" + x[2][26:])
                df["position_id"] = df["topics"].apply(lambda x: int(x[3], 16))
            else:
                df["pool_topics"] = df["pool_topics"].apply(lambda x: str(x).replace("\n", ","))
                df["proxy_topics"] = df["proxy_topics"].apply(lambda x: str(x).replace("\n", ","))
            file_name = os.path.join(data_save_path, utils.get_file_name(chain, contract_address, one_day))
            df.to_csv(file_name, header=True, index=False)
            file_names.append(file_name)
            pbar.update()

    return file_names


def download_pool_event_one_day(chain: BigQueryChain, contract_address, one_date) -> pd.DataFrame:
    client = bigquery.Client()
    query = f"""
SELECT block_number,block_timestamp, transaction_hash , transaction_index  as pool_tx_index, log_index pool_log_index, topics as pool_topics, DATA as pool_data, [] as proxy_topics, '' as proxy_data,null as proxy_log_index
        FROM {chain.value["table_name"]}
        WHERE  topics[SAFE_OFFSET(0)] in ('{constants.SWAP_KECCAK}')
            AND DATE(block_timestamp) >=  DATE("{one_date}") AND DATE(block_timestamp) <=  DATE("{one_date}") AND address = "{contract_address}"

union all

select pool.block_number, pool.block_timestamp, pool.transaction_hash, pool.transaction_index as pool_tx_index, pool.log_index as pool_log_index,pool.topics as pool_topics, pool.DATA as pool_data,  proxy.topics as proxy_topics, proxy.DATA as proxy_data, proxy.log_index as proxy_log_index from
(SELECT block_number, transaction_hash, block_timestamp, transaction_index, log_index, topics, DATA FROM {chain.value["table_name"]}
        WHERE topics[SAFE_OFFSET(0)] in ('{constants.MINT_KECCAK}')
            AND DATE(block_timestamp) >=  DATE("{one_date}") AND DATE(block_timestamp) <=  DATE("{one_date}") AND address = "{contract_address}") as pool
left join 
(SELECT transaction_hash, topics, DATA,log_index FROM  {chain.value["table_name"]}
        WHERE topics[SAFE_OFFSET(0)] in ('{constants.INCREASE_LIQUIDITY}')
            AND DATE(block_timestamp) >=  DATE("{one_date}") AND DATE(block_timestamp) <=  DATE("{one_date}") AND address = "{constants.PROXY_CONTRACT_ADDRESS}") as proxy
on pool.transaction_hash=proxy.transaction_hash 

union all

select pool.block_number, pool.block_timestamp, pool.transaction_hash, pool.transaction_index as pool_tx_index, pool.log_index as pool_log_index,pool.topics as pool_topics, pool.DATA as pool_data,  proxy.topics as proxy_topics, proxy.DATA as proxy_data, proxy.log_index as proxy_log_index from
(SELECT block_number, transaction_hash, block_timestamp, transaction_index, log_index, topics, DATA FROM {chain.value["table_name"]}
        WHERE topics[SAFE_OFFSET(0)] in ('{constants.COLLECT_KECCAK}')
            AND DATE(block_timestamp) >=  DATE("{one_date}") AND DATE(block_timestamp) <=  DATE("{one_date}") AND address = "{contract_address}") as pool
left join 
(SELECT transaction_hash, topics, DATA,log_index FROM  {chain.value["table_name"]}
        WHERE topics[SAFE_OFFSET(0)] in ('{constants.COLLECT}')
            AND DATE(block_timestamp) >=  DATE("{one_date}") AND DATE(block_timestamp) <=  DATE("{one_date}") AND address = "{constants.PROXY_CONTRACT_ADDRESS}") as proxy
on pool.transaction_hash=proxy.transaction_hash 

union all

select pool.block_number, pool.block_timestamp, pool.transaction_hash, pool.transaction_index as pool_tx_index, pool.log_index as pool_log_index,pool.topics as pool_topics, pool.DATA as pool_data,  proxy.topics as proxy_topics, proxy.DATA as proxy_data, proxy.log_index as proxy_log_index from
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
    result = result.sort_values(["block_number", "pool_log_index"], ascending=[True, True])
    return result

def download_proxy_event_one_day(chain: BigQueryChain, contract_address, one_date) -> pd.DataFrame:
    client = bigquery.Client()
    query = f"""
        SELECT
            block_number,
            transaction_hash,
            block_timestamp,
            log_index,
            topics

        FROM {chain.value["table_name"]}
        WHERE 1=1
            AND topics[SAFE_OFFSET(0)] ='{constants.TRANSFER_KECCAK}'
            AND DATE(block_timestamp) >=  DATE("{one_date}")
            AND DATE(block_timestamp) <=  DATE("{one_date}")
            AND address = "{contract_address}"
        order by block_number,log_index asc
    """
    query_job = client.query(query)  # Make an API request.
    result = query_job.to_dataframe(create_bqstorage_client=False)
    return result


