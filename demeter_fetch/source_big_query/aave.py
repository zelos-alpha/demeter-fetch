import os
from datetime import timedelta, date
from typing import List

import pandas as pd
from google.cloud import bigquery
from tqdm import tqdm

import demeter_fetch._typing as _typing
import demeter_fetch.constants as constants
import demeter_fetch.utils as utils
from .big_query_utils import BigQueryChain, set_environment, get_date_array


def download_event(
    chain: _typing.ChainType,
    tokens: List[str],
    date_begin: date,
    date_end: date,
    data_save_path: os.path,
    auth_file: str,
    http_proxy: str | None = None,
) -> List[str]:
    set_environment(auth_file, http_proxy)
    date_array = get_date_array(date_begin, date_end)
    file_names = []

    with tqdm(total=len(date_array), ncols=120) as pbar:
        for one_day in date_array:
            df = download_event_one_day(chain, one_day, tokens)
            df["token"] = df["topics"].apply(lambda x: utils.hex_to_length(x[1], 40))
            for token in tokens:
                token_df = df[df["token"] == token]
                file_name = os.path.join(
                    data_save_path, utils.get_aave_file_name(chain, token, one_day)
                )

                token_df.to_csv(file_name, header=True, index=False)
                file_names.append(file_name)
            pbar.update()

    return file_names


def download_event_one_day(
    chain: _typing.ChainType, one_date: date, tokens: List[str]
) -> pd.DataFrame:
    client = bigquery.Client()
    bq_chain_name = BigQueryChain[chain.name]
    token_str = ",".join(['"' + utils.hex_to_length(x, 64) + '"' for x in tokens])
    query = f"""
        SELECT block_number,transaction_hash,block_timestamp,transaction_index,log_index,topics,DATA
        FROM {bq_chain_name.value["table_name"]}
        WHERE
          topics[SAFE_OFFSET(0)] IN ('{constants.AAVE_RESERVE_DATA_UPDATED_KECCAK}')
          AND topics[SAFE_OFFSET(1)] IN ({token_str})
          AND DATE(block_timestamp) >= DATE("{one_date}")
          AND DATE(block_timestamp) <= DATE("{one_date}")
          AND address = "{_typing.ChainTypeConfig[chain]['aave_v3_pool_addr']}"
    """
    query_job = client.query(query)  # Make an API request.
    result = query_job.to_dataframe(create_bqstorage_client=False)
    result = result.sort_values(
        ["block_number", "log_index"], ascending=[True, True]
    )
    return result
