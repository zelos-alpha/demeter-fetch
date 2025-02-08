import os
import time
import subprocess
from datetime import date, datetime
from typing import Dict

import pandas as pd

from demeter_fetch import ChainType
from demeter_fetch.common import ApiUtil, print_log
from demeter_fetch.sources.source_utils import get_height_from_date
from .source_utils import ContractConfig


def check_chifra() -> bool:
    """
    check either chifra exist and works.
    :return: true:exist, false: not exist
    """
    cmd = f"chifra --version"
    ex = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, shell=True, stderr=subprocess.PIPE
    )
    out, err = ex.communicate()
    status = ex.wait()
    if err is None:
        return False
    # get terimal encoding
    master, slave = os.openpty()
    encoding = os.device_encoding(master)
    std_str: str = err.decode(encoding)
    exist = std_str.startswith("chifra version")
    if not exist:
        raise RuntimeError("Chifra is not installed or not configured")


def get_chifra_cmd(start_height, end_height, contract, topic0, output_file_name) -> str:
    return f"chifra export --logs --fmt csv --first_block {start_height} --last_block {end_height} {contract} {topic0} > {output_file_name}"


def _join_topic(row):
    # generate less object to make it faster
    if pd.isna(row["topic3"]):
        if pd.isna(row["topic2"]):
            if pd.isna(row["topic1"]):
                return [row["topic0"]]
            else:
                return [row["topic0"], row["topic1"]]
        else:
            return [row["topic0"], row["topic1"], row["topic2"]]
    else:
        return [row["topic0"], row["topic1"], row["topic2"], row["topic3"]]


def query_log_by_chifra(
    chain: ChainType,
    day: date,
    contract: ContractConfig,
    to_path=".",
    keep_tmp_files: bool = False,
    http_proxy="",
    etherscan_api_key="",
) -> pd.DataFrame:
    check_chifra()
    print_log(f"Exporting logs in {day} from chifra")
    topic0 = ""
    if len(contract.topics0) == 1:
        topic0 = contract.topics0[0]
    tmp_file_name = os.path.join(
        to_path, f"{contract.address}-{topic0}-{day}.chifra.csv"
    )
    if not os.path.exists(tmp_file_name) or os.path.getsize(tmp_file_name) == 0:
        start_height, end_height = get_height_from_date(
            day, chain, http_proxy, etherscan_api_key
        )
        cmd = get_chifra_cmd(
            start_height, end_height, contract.address, topic0, tmp_file_name
        )
        subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True).communicate()

    df = chifra_csv_to_raw_df(tmp_file_name, contract)
    if not keep_tmp_files:
        os.remove(tmp_file_name)
    return df


def chifra_csv_to_raw_df(path, contract: ContractConfig) -> pd.DataFrame:
    chifra_df = pd.read_csv(path)
    chifra_df = chifra_df[chifra_df["address"] == contract.address.lower()]
    chifra_df = chifra_df[chifra_df["topic0"].isin(contract.topics0)]

    chifra_df = chifra_df.rename(
        columns={
            "blockNumber": "block_number",
            "date": "block_timestamp",
            "transactionHash": "transaction_hash",
            "transactionIndex": "transaction_index",
            "logIndex": "log_index",
            "data": "data",
        }
    )
    chifra_df["block_timestamp"] = chifra_df["block_timestamp"].apply(
        lambda x: x.replace(" UTC", "")
    )
    chifra_df["topics"] = chifra_df.apply(_join_topic, axis=1)
    raw_df = chifra_df[
        [
            "block_number",
            "block_timestamp",
            "transaction_hash",
            "transaction_index",
            "log_index",
            "topics",
            "data",
        ]
    ]

    return raw_df
