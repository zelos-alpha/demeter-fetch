import json

import pandas as pd

from demeter_fetch import Config, ChainTypeConfig, constants
from demeter_fetch.constants import MINT_KECCAK, SWAP_KECCAK, BURN_KECCAK, COLLECT_KECCAK
from demeter_fetch.utils import print_log, get_file_name, UniswapUtil
from .chifra_utils import join_topic, save_by_day


def convert_chifra_csv_to_raw_file(
    config: Config,
):
    chifra_csv_path: str = config.from_config.chifra_config.file_path
    pool_addr: str = config.from_config.uniswap_config.pool_address
    to_path: str = config.to_config.save_path
    name_prefix: str = get_file_name(config.from_config.chain, config.from_config.uniswap_config.pool_address, "DAY")
    ignore_position_id: bool = config.from_config.chifra_config.ignore_position_id
    proxy_file_path: str = config.from_config.chifra_config.proxy_file_path
    if chifra_csv_path is None or chifra_csv_path == "":
        return []
    print_log("Loading csv file")
    pool_df = pd.read_csv(chifra_csv_path)
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
        proxy_df = pd.read_csv(proxy_file_path)
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
        UniswapUtil.match_proxy_log(pool_df, proxy_df)
        pool_df = pool_df.drop(["tx_type", "topic_name"], axis=1)

    return save_by_day(pool_df, to_path, name_prefix)
