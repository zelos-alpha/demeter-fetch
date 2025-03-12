import os
from datetime import date, timezone, datetime
from typing import List, Dict

import pandas as pd

import demeter_fetch.sources.rpc_utils as rpc_utils
from .source_utils import get_height_from_date
from .. import ChainType, ChainTypeConfig
from ..common import FromConfig, KECCAK, utils, split_topic, hex_to_length
from .source_utils import ContractConfig


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
    else:
        columns = [
            "block_number",
            "transaction_hash",
            "transaction_index",
            "log_index",
            "topics",
            "data",
        ]
    df = df[columns]
    return df


# def query_tx_logs(
#     chain: ChainType,
#     end_point: str,
#     save_path: str,
#     auth_string: str | None = None,
#     http_proxy: str | None = None,
# ) -> pd.DataFrame:
#     client = rpc_utils.EthRpcClient(end_point, http_proxy, auth_string)
#     utils.print_log(f"Will download transaction logs of uniswap")
#
#     pass


def query_logs(
    chain: ChainType,
    end_point: str,
    save_path: str,
    start_height: int,
    end_height: int,
    contract: ContractConfig,
    batch_size: int = 500,
    auth_string: str | None = None,
    http_proxy: str | None = None,
    keep_tmp_files: bool = False,
    one_by_one: bool = False,
    skip_timestamp: bool = False,
    height_cache_path: str = None,
    thread: int = 10,
) -> pd.DataFrame:
    client = rpc_utils.EthRpcClient(end_point, http_proxy, auth_string)
    utils.print_log(f"Will download from height {start_height} to {end_height}")
    try:
        tmp_files_paths: List[str] = rpc_utils.query_event_by_height_concurrent(
            chain,
            client,
            contract,
            start_height,
            end_height,
            save_path=save_path,
            batch_size=batch_size,
            one_by_one=one_by_one,
            skip_timestamp=skip_timestamp,
            height_cache_path=height_cache_path,
            thread=thread,
        )
    except Exception as e:
        print(e)
        import traceback

        print(traceback.format_exc())
        exit(1)

    current_day_logs = []
    # Load temporary files based on height, then reorganize into raw files by day
    # Note: The logs in the tmp file have been sorted
    # utils.print_log("Generating daily files")
    for tmp_file in tmp_files_paths:
        logs: List[Dict] = rpc_utils.load_tmp_file(tmp_file)
        current_day_logs.extend(logs)
    df = pd.DataFrame(current_day_logs)
    if "block_dt" in df.columns:
        df = df.drop(columns=["block_dt"])
    if len(df.index) < 1:
        df = pd.DataFrame(
            columns=[
                "block_number",
                "block_timestamp",
                "transaction_hash",
                "transaction_index",
                "log_index",
                "topics",
                "data",
            ]
        )
    else:
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
        contract=ContractConfig(
            config.uniswap_config.pool_address,
            [KECCAK.SWAP.value, KECCAK.BURN.value, KECCAK.COLLECT.value, KECCAK.MINT.value],
        ),
        batch_size=config.rpc.batch_size,
        auth_string=config.rpc.auth_string,
        http_proxy=config.http_proxy if not config.rpc.force_no_proxy else None,
        keep_tmp_files=config.rpc.keep_tmp_files,
        one_by_one=False,
        skip_timestamp=False,
        height_cache_path=config.rpc.height_cache_path,
        thread=config.rpc.thread,
    )
    daily_df = _update_df(daily_df)
    return daily_df


def rpc_uni_v4_pool(config: FromConfig, save_path: str, day: date) -> pd.DataFrame:
    start_height, end_height = get_height_from_date(day, config.chain, config.http_proxy, config.rpc.etherscan_api_key)
    daily_df = query_logs(
        chain=config.chain,
        end_point=config.rpc.end_point,
        save_path=save_path,
        start_height=start_height,
        end_height=end_height,
        contract=ContractConfig(
            ChainTypeConfig[config.chain]["uni_v4_pool_manager"],
            [
                KECCAK.UNI_V4_SWAP.value,
                KECCAK.UNI_V4_MODIFY_LIQ.value,
            ],
            [config.uniswap_config.pool_address],
        ),
        batch_size=config.rpc.batch_size,
        auth_string=config.rpc.auth_string,
        http_proxy=config.http_proxy if not config.rpc.force_no_proxy else None,
        keep_tmp_files=config.rpc.keep_tmp_files,
        one_by_one=False,
        skip_timestamp=False,
        height_cache_path=config.rpc.height_cache_path,
        thread=config.rpc.thread,
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
        contract=ContractConfig(
            ChainTypeConfig[config.chain]["uniswap_proxy_addr"],
            [
                KECCAK.UNI_PROXY_DECREASE.value,
                KECCAK.UNI_PROXY_INCREASE.value,
                KECCAK.UNI_PROXY_COLLECT.value,
            ],
        ),
        batch_size=config.rpc.batch_size,
        auth_string=config.rpc.auth_string,
        http_proxy=config.http_proxy if not config.rpc.force_no_proxy else None,
        keep_tmp_files=config.rpc.keep_tmp_files,
        one_by_one=False,
        skip_timestamp=False,
        height_cache_path=config.rpc.height_cache_path,
        thread=config.rpc.thread,
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
        contract=ContractConfig(
            ChainTypeConfig[config.chain]["uniswap_proxy_addr"],
            [KECCAK.TRANSFER.value],
        ),
        batch_size=config.rpc.batch_size,
        auth_string=config.rpc.auth_string,
        http_proxy=config.http_proxy if not config.rpc.force_no_proxy else None,
        keep_tmp_files=config.rpc.keep_tmp_files,
        one_by_one=True,
        skip_timestamp=True,
        height_cache_path=config.rpc.height_cache_path,
        thread=config.rpc.thread,
    )
    daily_df = _update_df(daily_df)
    return daily_df


def rpc_uni_tx(config: FromConfig, tx_hashes: pd.Series) -> pd.DataFrame:
    http_proxy = config.http_proxy if not config.rpc.force_no_proxy else None
    client = rpc_utils.EthRpcClient(config.rpc.end_point, http_proxy, config.rpc.auth_string)
    df = rpc_utils.query_tx(client, tx_hashes)
    # df = df.drop(columns=["from", "to"])
    return df


def rpc_aave(config: FromConfig, save_path: str, day: date, tokens):
    start_height, end_height = get_height_from_date(day, config.chain, config.http_proxy, config.rpc.etherscan_api_key)
    daily_df = query_logs(
        chain=config.chain,
        end_point=config.rpc.end_point,
        save_path=save_path,
        start_height=start_height,
        end_height=end_height,
        contract=ContractConfig(
            ChainTypeConfig[config.chain]["aave_v3_pool_addr"],
            [
                KECCAK.AAVE_REPAY.value,
                KECCAK.AAVE_BORROW.value,
                KECCAK.AAVE_SUPPLY.value,
                KECCAK.AAVE_WITHDRAW.value,
                KECCAK.AAVE_UPDATED.value,
                KECCAK.AAVE_LIQUIDATION.value,
            ],
        ),
        batch_size=config.rpc.batch_size,
        auth_string=config.rpc.auth_string,
        http_proxy=config.http_proxy if not config.rpc.force_no_proxy else None,
        keep_tmp_files=config.rpc.keep_tmp_files,
        one_by_one=False,
        skip_timestamp=False,
        height_cache_path=config.rpc.height_cache_path,
        thread=config.rpc.thread,
    )
    daily_df = _update_df(daily_df)
    daily_df["topics"] = daily_df["topics"].apply(lambda x: split_topic(x))
    daily_df["token"] = daily_df["topics"].apply(lambda r: hex_to_length(r[1], 40))
    daily_df = daily_df[daily_df["token"].isin(tokens)]
    return daily_df


def rpc_squeeth(config: FromConfig, save_path: str, day: date) -> pd.DataFrame:
    if "squeeth_controller" not in ChainTypeConfig[config.chain]:
        raise RuntimeError(f"Squeeth does not exist in chain {config.chain.name}")
    start_height, end_height = get_height_from_date(day, config.chain, config.http_proxy, config.rpc.etherscan_api_key)
    daily_df = query_logs(
        chain=config.chain,
        end_point=config.rpc.end_point,
        save_path=save_path,
        start_height=start_height,
        end_height=end_height,
        contract=ContractConfig(
            ChainTypeConfig[config.chain]["squeeth_controller"],
            [KECCAK.SQUEETH_NORM_FACTOR_UPDATED.value],
        ),
        batch_size=config.rpc.batch_size,
        auth_string=config.rpc.auth_string,
        http_proxy=config.http_proxy if not config.rpc.force_no_proxy else None,
        keep_tmp_files=config.rpc.keep_tmp_files,
        one_by_one=True,
        skip_timestamp=True,
        height_cache_path=config.rpc.height_cache_path,
        thread=config.rpc.thread,
    )
    daily_df = _update_df(daily_df)
    daily_df["block_timestamp"] = daily_df["data"].apply(
        lambda x: datetime.fromtimestamp(int(x[64 * 3 + 2 :], 16), tz=timezone.utc)
    )

    return daily_df


def rpc_gmx_v2(config: FromConfig, save_path: str, day: date):
    start_height, end_height = get_height_from_date(day, config.chain, config.http_proxy, config.rpc.etherscan_api_key)
    daily_df = query_logs(
        chain=config.chain,
        end_point=config.rpc.end_point,
        save_path=save_path,
        start_height=start_height,
        end_height=end_height,
        contract=ContractConfig(ChainTypeConfig[config.chain]["gmx_event_emitter"], []),
        batch_size=config.rpc.batch_size,
        auth_string=config.rpc.auth_string,
        http_proxy=config.http_proxy if not config.rpc.force_no_proxy else None,
        keep_tmp_files=config.rpc.keep_tmp_files,
        one_by_one=False,
        skip_timestamp=True,
        height_cache_path=config.rpc.height_cache_path,
        thread=config.rpc.thread,
    )
    daily_df = _update_df(daily_df)
    return daily_df
