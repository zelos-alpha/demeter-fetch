import csv
import json
import os.path
import time
from datetime import date, datetime
from datetime import timezone
from typing import List, Dict

import pandas as pd
import requests

import demeter_fetch.constants as constants
import demeter_fetch.utils as utils
from ._typing import ChainType, ChainTypeConfig, OnchainTxType
from .eth_rpc_client import EthRpcClient, query_event_by_height, ContractConfig, load_tmp_file
from .uniswap_utils import compare_burn_data
from tqdm import tqdm  # process bar
from .utils import print_log

"""
通过rpc下载, event log, 并管理时间-高度的缓存.

"""


def query_blockno_from_time(chain: ChainType, blk_time: datetime, is_before: bool = True, proxy=""):
    proxies = {"http": proxy, "https": proxy, } if proxy else {}
    before_or_after = "before" if is_before else "after"
    url = utils.ChainTypeConfig[chain]["query_height_api"]
    blk_time = blk_time.replace(tzinfo=timezone.utc)
    result = requests.get(url.replace("%1", str(int(blk_time.timestamp()))).replace("%2", before_or_after), proxies=proxies)
    if result.status_code != 200:
        raise RuntimeError("request block number failed, code: " + str(result.status_code))
    result_json = result.json()
    if int(result_json["status"]) == 1:
        return int(result_json["result"])
    else:
        raise RuntimeError("request block number failed, message: " + str(result_json))


def query_uniswap_pool_logs(chain: ChainType,
                            pool_addr: str,
                            end_point: str,
                            save_path: str,
                            start: date = None,
                            end: date = None,
                            start_height: int = None,
                            end_height: int = None,
                            batch_size: int = 500,
                            auth_string: str | None = None,
                            http_proxy: str | None = None,
                            keep_tmp_files: bool = False):
    # 从start and end 时间获取高度
    if start_height is None and end_height is None:
        if start is None and end is None:
            raise RuntimeError("Ether fill start/end date or start end height")
        start_height = query_blockno_from_time(chain, datetime.combine(start, datetime.min.time()), False, http_proxy)
        utils.print_log("Querying end timestamp, wait for 8 seconds to prevent max rate limit")
        time.sleep(8)  # to prevent request limit
        end_height = query_blockno_from_time(chain, datetime.combine(end, datetime.max.time()), True, http_proxy)

    # 通过eth rpc下载, 并获得按照高度划分的event临时文件列表
    client = EthRpcClient(end_point, http_proxy, auth_string)
    utils.print_log(f"Will download from height {start_height} to {end_height}")
    try:
        tmp_files_paths: List[str] = query_event_by_height(chain,
                                                           client,
                                                           ContractConfig(pool_addr,
                                                                          [constants.SWAP_KECCAK,
                                                                           constants.BURN_KECCAK,
                                                                           constants.COLLECT_KECCAK,
                                                                           constants.MINT_KECCAK]),
                                                           start_height,
                                                           end_height,
                                                           save_path=save_path,
                                                           batch_size=batch_size,
                                                           one_by_one=False)
    except Exception as e:
        print(e)
        import traceback
        print(traceback.format_exc())
        exit(1)

    # 根据高度加载临时文件, 然后按天重新组织成raw文件
    # 注意: tmp文件中的log是已经排序过的
    current_day: date | None = None
    current_day_logs = []
    raw_file_list = []
    print_log("generate daily files")
    for tmp_file in tmp_files_paths:
        logs: List[Dict] = load_tmp_file(tmp_file)
        for log in logs:
            log_day = log["block_dt"].date()
            if not current_day:
                current_day = log_day
            if log_day != current_day:  # save current day logs to file
                raw_file_list.append(_save_one_day(save_path, current_day, pool_addr, current_day_logs, chain))
                current_day_logs = []
                current_day = log_day
                print_log(f"save raw file in day {str(current_day)}, log count: {len(current_day_logs)}")
            del log["block_dt"]  # append to write
            log["pool_tx_index"] = log.pop("transaction_index")
            log["pool_log_index"] = log.pop("log_index")
            log["pool_topics"] = log.pop("topics")
            log["pool_data"] = log.pop("data")
            current_day_logs.append(log)

    # save rest to file
    raw_file_list.append(_save_one_day(save_path, current_day, pool_addr, current_day_logs, chain))
    # remove tmp files
    if not keep_tmp_files:
        [os.remove(f) for f in tmp_files_paths]
    return raw_file_list, start_height, end_height


def append_proxy_file(raw_file_list: List[str],
                      start_height: int,
                      end_height: int,
                      chain: ChainType,
                      end_point: str,
                      save_path: str,
                      batch_size: int = 500,
                      auth_string: str | None = None,
                      http_proxy: str | None = None,
                      keep_tmp_files: bool = False):
    # download logs first
    client = EthRpcClient(end_point, http_proxy, auth_string)
    tmp_files_paths: List[str] = query_event_by_height(chain,
                                                       client,
                                                       ContractConfig(ChainTypeConfig[chain]["proxy_addr"],
                                                                      [constants.INCREASE_LIQUIDITY,
                                                                       constants.DECREASE_LIQUIDITY,
                                                                       constants.COLLECT]),
                                                       start_height,
                                                       end_height,
                                                       save_path=save_path,
                                                       batch_size=batch_size,
                                                       one_by_one=True,
                                                       skip_timestamp=True)
    print_log("start merge pool and proxy files")
    with tqdm(total=len(raw_file_list), ncols=150) as pbar:
        # merge logs to file
        for raw_file_path in raw_file_list:
            # load tmp file in match height
            daily_pool_logs: pd.DataFrame = pd.read_csv(raw_file_path)
            day_start_height = daily_pool_logs.head(1)["block_number"].iloc[0]
            day_end_height = daily_pool_logs.tail(1)["block_number"].iloc[0]
            # load tmp files
            matched_tmp_files = _find_matched_tmp_file(day_start_height, day_end_height, tmp_files_paths)
            proxy_log_list = []
            for tmp_file in matched_tmp_files:
                proxy_log_list.extend(load_tmp_file(tmp_file))
            # match proxy logs to pool logs
            proxy_log_df: pd.DataFrame = pd.DataFrame(proxy_log_list)
            proxy_log_df.set_index("transaction_hash", inplace=True)
            proxy_log_df = _process_topic(proxy_log_df, True)
            daily_pool_logs = _process_topic(daily_pool_logs)
            daily_pool_logs["tx_type"] = daily_pool_logs.apply(lambda x: constants.type_dict[x.topic_array[0]], axis=1)
            _match_proxy_log(daily_pool_logs, proxy_log_df)

            # save new raw files
            daily_pool_logs = daily_pool_logs.drop(["tx_type", "topic_array", "topic_name"], axis=1)

            order = ["block_number", "transaction_hash", "block_timestamp", "pool_tx_index", "pool_log_index",
                     "pool_topics", "pool_data", "proxy_topics", "proxy_data", "proxy_log_index"]
            daily_pool_logs = daily_pool_logs[order]
            # print(daily_pool_logs)
            daily_pool_logs.to_csv(raw_file_path, index=False)
            pbar.update()
    # remove tmp files
    if not keep_tmp_files:
        [os.remove(f) for f in tmp_files_paths]


def _process_topic(df, is_proxy=False):
    if is_proxy:
        df["topic_array"] = df.apply(lambda x: json.loads(x.topics), axis=1)
    else:
        df["topic_array"] = df.apply(lambda x: json.loads(x.pool_topics), axis=1)
    df["topic_name"] = df.apply(lambda x: x.topic_array[0], axis=1)
    return df


def _add_proxy_log(df, index, proxy_row):
    df.loc[index, "proxy_data"] = proxy_row.data
    df.loc[index, "proxy_topics"] = proxy_row.topics
    df.loc[index, "proxy_log_index"] = proxy_row.log_index


def _match_proxy_log(pool_logs: pd.DataFrame, proxy_logs: pd.DataFrame):
    for index, row in pool_logs.iterrows():
        if row.tx_type == OnchainTxType.SWAP:
            continue
        if row.transaction_hash not in proxy_logs.index:
            continue
        proxy_tx: pd.DataFrame = proxy_logs.loc[[row.transaction_hash]]
        proxy_tx_matched: pd.DataFrame = proxy_tx.loc[proxy_tx.topic_name == constants.topic_dict[row.topic_name]]

        for pindex, possible_match in proxy_tx_matched.iterrows():
            if row.tx_type == OnchainTxType.MINT:
                if row.pool_data[66:] == possible_match.data[2:]:
                    _add_proxy_log(pool_logs, index, possible_match)
                    break
            elif row.tx_type == OnchainTxType.COLLECT or row.tx_type == OnchainTxType.BURN:
                if compare_burn_data(row.pool_data, possible_match.data):
                    _add_proxy_log(pool_logs, index, possible_match)
                    break
            else:
                raise ValueError("not support tx type")
    # if no column is generated
    if "proxy_topics" not in pool_logs.columns:
        pool_logs["proxy_data"] = None
        pool_logs["proxy_topics"] = [[]] * pool_logs.shape[0]
        pool_logs["proxy_log_index"] = None
    else:
        pool_logs["proxy_topics"] = pool_logs["proxy_topics"].fillna("[]")


def _find_matched_tmp_file(start, end, tmp_files):
    s_list = []
    e_list = []
    for index, fname in enumerate(tmp_files):
        head, tail = os.path.split(fname)
        tail = tail.replace(".tmp.pkl", "").split("-")
        s_list.append(int(tail[2]))
        e_list.append(int(tail[3]))

    begin_index = s_list.index(start + max(filter(lambda y: y <= 0, map(lambda x: x - start, s_list))))
    end_index = e_list.index(end + min(filter(lambda y: y >= 0, map(lambda x: x - end, e_list))))
    return tmp_files[begin_index:end_index + 1]


def _save_one_day(save_path: str, day: date, contract_address: str, one_day_data: List[Dict], chain: ChainType):
    full_path = os.path.join(save_path, utils.get_file_name(chain, contract_address, day, True))
    with open(full_path, 'w') as csvfile:
        writer = csv.DictWriter(csvfile,
                                fieldnames=['block_number', 'block_timestamp', 'transaction_hash', 'pool_tx_index', 'pool_log_index', 'pool_topics', 'pool_data'])
        writer.writeheader()
        for item in one_day_data:
            writer.writerow(item)
    return full_path

# def dict_factory(cursor, row):
#     d = {}
#     for idx, col in enumerate(cursor.description):
#         d[col[0]] = row[idx]
#     return d
