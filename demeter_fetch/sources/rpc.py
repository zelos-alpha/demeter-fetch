import csv
import os
import time
from datetime import date, datetime
from typing import List, Dict

import pandas as pd

from .. import ChainType, ChainTypeConfig
from ..common import FromConfig, Config, constants
from .big_query_utils import BigQueryChain, query_by_sql
import demeter_fetch.common.utils as utils
import demeter_fetch.sources.rpc_utils as rpc_utils


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


def _update_df(df: pd.DataFrame) -> pd.DataFrame:
    if "block_timestamp" in df.columns:
        df["block_timestamp"] = df["block_timestamp"].apply(lambda x: x.replace("T", " "))

        columns = [
            "block_number",
            "block_timestamp",
            "transaction_hash",
            "transaction_index",
            "log_index",
            "topics",
            "data",
        ]
        df = df[columns]
    return df


def query_logs(
    chain: ChainType,
    end_point: str,
    save_path: str,
    start_height: int,
    end_height: int,
    contract: rpc_utils.ContractConfig,
    batch_size: int = 500,
    auth_string: str | None = None,
    http_proxy: str | None = None,
    keep_tmp_files: bool = False,
    one_by_one: bool = False,
    skip_timestamp: bool = False,
):
    client = rpc_utils.EthRpcClient(end_point, http_proxy, auth_string)
    utils.print_log(f"Will download from height {start_height} to {end_height}")
    try:
        tmp_files_paths: List[str] = rpc_utils.query_event_by_height(
            chain,
            client,
            contract,
            start_height,
            end_height,
            save_path=save_path,
            batch_size=batch_size,
            one_by_one=one_by_one,
            skip_timestamp=skip_timestamp,
        )
    except Exception as e:
        print(e)
        import traceback

        print(traceback.format_exc())
        exit(1)

    current_day_logs = []
    # Load temporary files based on height, then reorganize into raw files by day
    # Note: The logs in the tmp file have been sorted
    utils.print_log("Generating daily files")
    for tmp_file in tmp_files_paths:
        logs: List[Dict] = rpc_utils.load_tmp_file(tmp_file)
        current_day_logs.extend(logs)
    df = pd.DataFrame(current_day_logs)
    if "block_dt" in df.columns:
        df = df.drop(columns=["block_dt"])
    df = df.sort_values(["block_number", "log_index"], ascending=[True, True])
    # remove tmp files
    if not keep_tmp_files:
        for f in tmp_files_paths:
            if os.path.exists(f):
                os.remove(f)
    return df


def rpc_pool(config: FromConfig, save_path: str, day: date) -> pd.DataFrame:
    start_height, end_height = get_height_from_date(day, config.chain, config.http_proxy, config.rpc.etherscan_api_key)
    daily_df = query_logs(
        chain=config.chain,
        end_point=config.rpc.end_point,
        save_path=save_path,
        start_height=start_height,
        end_height=end_height,
        contract=rpc_utils.ContractConfig(
            config.uniswap_config.pool_address,
            [constants.SWAP_KECCAK, constants.BURN_KECCAK, constants.COLLECT_KECCAK, constants.MINT_KECCAK],
        ),
        batch_size=config.rpc.batch_size,
        auth_string=config.rpc.auth_string,
        http_proxy=config.http_proxy,
        keep_tmp_files=config.rpc.keep_tmp_files,
        one_by_one=False,
        skip_timestamp=False,
    )
    daily_df = _update_df(daily_df)
    return daily_df


def rpc_proxy_lp(config: FromConfig, save_path: str, day: date) -> pd.DataFrame:
    start_height, end_height = get_height_from_date(day, config.chain, config.http_proxy, config.rpc.etherscan_api_key)
    daily_df = query_logs(
        chain=config.chain,
        end_point=config.rpc.end_point,
        save_path=save_path,
        start_height=start_height,
        end_height=end_height,
        contract=rpc_utils.ContractConfig(
            ChainTypeConfig[config.chain]["uniswap_proxy_addr"],
            [constants.INCREASE_LIQUIDITY, constants.DECREASE_LIQUIDITY, constants.COLLECT],
        ),
        batch_size=config.rpc.batch_size,
        auth_string=config.rpc.auth_string,
        http_proxy=config.http_proxy,
        keep_tmp_files=config.rpc.keep_tmp_files,
        one_by_one=False,
        skip_timestamp=True,
    )
    daily_df = _update_df(daily_df)
    return daily_df


def rpc_proxy_transfer(config: FromConfig, save_path: str, day: date) -> pd.DataFrame:
    start_height, end_height = get_height_from_date(day, config.chain, config.http_proxy, config.rpc.etherscan_api_key)
    daily_df = query_logs(
        chain=config.chain,
        end_point=config.rpc.end_point,
        save_path=save_path,
        start_height=start_height,
        end_height=end_height,
        contract=rpc_utils.ContractConfig(
            ChainTypeConfig[config.chain]["uniswap_proxy_addr"],
            [constants.TRANSFER_KECCAK],
        ),
        batch_size=config.rpc.batch_size,
        auth_string=config.rpc.auth_string,
        http_proxy=config.http_proxy,
        keep_tmp_files=config.rpc.keep_tmp_files,
        one_by_one=True,
        skip_timestamp=True,
    )
    daily_df = _update_df(daily_df)
    return daily_df
