import os
import json
import time
import subprocess
from datetime import timedelta, date, datetime
from typing import List, Dict

import pandas as pd
from tqdm import tqdm
import demeter_fetch._typing as _typing

from demeter_fetch import Config, ChainTypeConfig, constants
from demeter_fetch.constants import MINT_KECCAK, SWAP_KECCAK, BURN_KECCAK, COLLECT_KECCAK, PROXY_CONTRACT_ADDRESS
from demeter_fetch.utils import print_log, get_file_name, UniswapUtil, ApiUtil, TimeUtil
from .chifra_utils import join_topic, save_by_day


def convert_chifra_csv_to_raw_file(
    config: Config,
):
    pool_csv_path: str = config.from_config.chifra_config.file_path
    pool_addr: str = config.from_config.uniswap_config.pool_address
    proxy_addr: str = PROXY_CONTRACT_ADDRESS
    to_path: str = config.to_config.save_path
    name_prefix: str = get_file_name(config.from_config.chain, config.from_config.uniswap_config.pool_address, "DAY")
    ignore_position_id: bool = config.from_config.chifra_config.ignore_position_id
    proxy_file_path: str = config.from_config.chifra_config.proxy_file_path
    print_log("Loading csv file")
    if config.from_config.chifra_config.start and config.from_config.chifra_config.end:
        if not os.listdir(pool_csv_path) or not os.listdir(pool_csv_path):
            return []
        days = TimeUtil.get_date_array(config.from_config.chifra_config.start, config.from_config.chifra_config.end)
        dt_strs = [day.strftime("%Y-%m-%d") for day in days]
        pool_df = pd.DataFrame()
        for dt_str in dt_strs:
            df_pool = pd.read_csv(f'{pool_csv_path}/{pool_addr}_{dt_str}.raw.csv', sep='\t')
            pool_df = pd.concat([pool_df, df_pool])
    else:
        if pool_csv_path is None or pool_csv_path == "":
            return []
        pool_df = pd.read_csv(pool_csv_path, sep='\t')
    print_log("Process files")
    pool_df = pool_df[pool_df["address"] == pool_addr.lower()]
    pool_df = pool_df[pool_df["topic0"].isin([MINT_KECCAK, SWAP_KECCAK, BURN_KECCAK, COLLECT_KECCAK])]

    pool_df = pool_df.rename(
        columns={
            "blockNumber": "block_number",
            "date": "block_timestamp",
            "transactionHash": "transaction_hash",
            "transactionIndex": "pool_tx_index",
            "logIndex": "pool_log_index",
            "data": "pool_data",
        }
    )
    pool_df["block_timestamp"] = pool_df["block_timestamp"].apply(lambda x: x.replace(" UTC", "+00:00"))
    pool_df["pool_topics"] = pool_df.apply(join_topic, axis=1)
    pool_df = pool_df[["block_number", "block_timestamp", "transaction_hash", "pool_tx_index", "pool_log_index", "pool_topics", "pool_data"]]
    if ignore_position_id:
        pool_df["proxy_topics"] = None
        pool_df["proxy_data"] = None
        pool_df["proxy_log_index"] = None
    else:
        print_log("Pool logs has downloaded, now will convert proxy logs")
        print_log("loading proxy file")
        if config.from_config.chifra_config.start and config.from_config.chifra_config.end:
            days = TimeUtil.get_date_array(config.from_config.chifra_config.start, config.from_config.chifra_config.end)
            dt_strs = [day.strftime("%Y-%m-%d") for day in days]
            proxy_df = pd.DataFrame()
            for dt_str in dt_strs:
                df = pd.read_csv(f'{proxy_file_path}/{proxy_addr}_{dt_str}.raw.csv', sep='\t')
                proxy_df = pd.concat([proxy_df, df])
        else:
            proxy_df = pd.read_csv(proxy_file_path, sep='\t')
        proxy_df = proxy_df[proxy_df["address"] == ChainTypeConfig[config.from_config.chain]["uniswap_proxy_addr"]]
        proxy_df["topics"] = proxy_df.apply(join_topic, axis=1)
        proxy_df = proxy_df.rename(
            columns={
                "blockNumber": "block_number",
                "date": "block_timestamp",
                "transactionHash": "transaction_hash",
                "transactionIndex": "tx_index",
                "logIndex": "log_index",
                "topic0": "topic_name",
            }
        )
        pool_df["topic_name"] = pool_df["pool_topics"].apply(lambda x: x[0])
        pool_df["tx_type"] = pool_df["topic_name"].apply(lambda x: constants.type_dict[x])
        proxy_df.set_index("transaction_hash", inplace=True)

        print_log("Matching proxy log to pool logs, this may take a while")

        pool_df["day"] = pool_df["block_timestamp"].apply(lambda x: x[0:10])
        pool_df_grouped = pool_df.groupby(["day"])
        proxy_df["day"] = proxy_df["block_timestamp"].apply(lambda x: x[0:10])
        proxy_df_grouped = proxy_df.groupby(["day"])
        processed = []
        with tqdm(total=pool_df_grouped.ngroups, ncols=150) as pbar1:
            for day_str, pool_day_df in pool_df_grouped:
                proxy_day_df = proxy_df_grouped.get_group(day_str[0])
                UniswapUtil.match_proxy_log(pool_day_df, proxy_day_df)
                processed.append(pool_day_df)
                pbar1.update()
        pool_df = pd.concat(processed)
        pool_df = pool_df.drop(["tx_type", "topic_name", "day"], axis=1)
    print_log("Start to save files")
    return save_by_day(pool_df, to_path, name_prefix)

def download_event(
    chain: _typing.ChainType,
    contract_address: str,
    to_file_list:Dict[date, str],
    data_save_path: os.path,
    key: str,
    http_proxy: str | None = None,
):
    with tqdm(total=len(to_file_list), ncols=120) as pbar:
        for one_day in to_file_list.keys():
            download_event_one_day(chain, contract_address, one_day, key, http_proxy, data_save_path)
            pbar.update()

def download_event_one_day(chain, contract, one_day, key, http_proxy, data_save_path) -> pd.DataFrame:
    start = end = one_day
    start_height = ApiUtil.query_blockno_from_time(
        chain,
        datetime.combine(start, datetime.min.time()),
        False,
        http_proxy,
        key,
    )
    sleep_time = 8
    time.sleep(sleep_time)  # to prevent request limit
    end_height = ApiUtil.query_blockno_from_time(
        chain,
        datetime.combine(end, datetime.max.time()),
        True,
        http_proxy,
        key,
    )
    print_log(f'fetch start_height: {start_height}, end_height: {end_height}')
    cmd = f'chifra export --logs --first_block {start_height} --last_block {end_height} {contract} > {data_save_path}/{contract}_{one_day}.raw.csv'
    subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True).communicate()
    return pd.DataFrame()
