import os
import pandas as pd
from datetime import datetime, tzinfo
from demeter_fetch.constants import MINT_KECCAK, SWAP_KECCAK, BURN_KECCAK, COLLECT_KECCAK
from .chifra_utils import join_topic, save_by_day


def convert_chifra_csv_to_raw_file(chifra_csv_path: str, pool_addr: str, to_path: str, name_prefix: str):
    if chifra_csv_path is None or chifra_csv_path == "":
        return []
    chifra_df = pd.read_csv(chifra_csv_path)
    chifra_df = chifra_df[chifra_df["address"] == pool_addr.lower()]
    chifra_df = chifra_df[chifra_df["topic0"].isin([MINT_KECCAK, SWAP_KECCAK, BURN_KECCAK, COLLECT_KECCAK])]

    chifra_df = chifra_df.rename(
        columns={
            "blockNumber": "block_number",
            "date": "block_timestamp",
            "transactionHash": "transaction_hash",
            "transactionIndex": "pool_tx_index",
            "logIndex": "pool_log_index",
            "data": "pool_data",
        }
    )
    chifra_df["block_timestamp"] = chifra_df["block_timestamp"].apply(lambda x: x.replace(" UTC", "+00:00"))
    chifra_df["pool_topics"] = chifra_df.apply(join_topic, axis=1)
    # block_number,block_timestamp,transaction_hash,pool_tx_index,pool_log_index,pool_topics,pool_data,proxy_topics,proxy_data,proxy_log_index
    chifra_df = chifra_df[["block_number", "block_timestamp", "transaction_hash", "pool_tx_index", "pool_log_index", "pool_topics", "pool_data"]]

    return save_by_day(chifra_df, to_path, name_prefix)
