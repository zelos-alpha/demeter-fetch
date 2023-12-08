import os.path
from datetime import date, datetime
from typing import Tuple

import pandas as pd
from tqdm import tqdm

from demeter_fetch import FromConfig, DappType, utils, ChainType, ChainTypeConfig
from demeter_fetch.utils import print_log


def get_export_commend(from_config: FromConfig) -> Tuple[str, int, int]:
    match from_config.dapp_type:
        case DappType.uniswap:
            contract_addr = from_config.uniswap_config.pool_address
        case _:
            raise RuntimeError(f"Unsupported dapp type {from_config.dapp_type}")

    start_height = utils.ApiUtil.query_blockno_from_time(
        from_config.chain,
        datetime.combine(from_config.chifra_config.start, datetime.min.time()),
        False,
        from_config.http_proxy,
        from_config.chifra_config.etherscan_api_key,
    )
    end_height = utils.ApiUtil.query_blockno_from_time(
        from_config.chain,
        datetime.combine(from_config.chifra_config.end, datetime.max.time()),
        True,
        from_config.http_proxy,
        from_config.chifra_config.etherscan_api_key,
    )
    output_file_name = f"{from_config.chain.value}_{contract_addr}_height-{start_height}-{end_height}.csv"
    return (
        f"chifra export --logs --fmt csv --first_block {start_height} --last_block {end_height} {contract_addr} > {output_file_name}",
        start_height,
        end_height,
    )


def get_uniswap_export_commend(chain: ChainType, start: int, end: int):
    contract_addr = ChainTypeConfig[chain]["uniswap_proxy_addr"]
    output_file_name = f"{chain.value}_{contract_addr}_height-{start}-{end}.csv"
    return f"chifra export --logs --fmt csv --first_block {start} --last_block {end} {contract_addr}> {output_file_name}"


def join_topic(row):
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
    # topic_str = f"'{row['topic0']}','{row['topic1']}','{row['topic2']}','{row['topic3']}'"
    # return "[" + topic_str.replace(",'nan'", "") + "]"  # will not replace topic0, because it's never null


def save_by_day(df: pd.DataFrame, save_path: str, name_prefix: str):
    print_log("Saving daily files")
    df["day"] = df["block_timestamp"].apply(lambda x: x[0:10])
    df_grouped = df.groupby(["day"])
    file_name_list = []
    with tqdm(total=df_grouped.ngroups, ncols=150) as pbar1:
        for day_str, day_df in df_grouped:
            day_df = day_df.sort_values(["block_number", "pool_log_index"])
            day_df = day_df.drop(columns=["day"])
            fname = name_prefix.replace("DAY", day_str[0])
            day_df.to_csv(os.path.join(save_path, fname), index=False)
            file_name_list.append(os.path.join(save_path, fname))
            pbar1.update()
    return file_name_list
