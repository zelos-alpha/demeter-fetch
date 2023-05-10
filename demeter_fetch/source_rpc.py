import json
import csv
import os.path
import pickle
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from operator import itemgetter
from typing import List, Dict
from datetime import timezone
import requests
from tqdm import tqdm  # process bar

import demeter_fetch.constants as constants
import demeter_fetch.utils as utils
from ._typing import ChainType
from .eth_rpc_client import EthRpcClient, GetLogsParam

"""
通过rpc下载, event log, 并管理时间-高度的缓存.

"""


@dataclass
class ContractConfig:
    address: str
    topics: List[str]


class HeightCacheManager:
    """
    高度缓存
    """
    height_cache_file_name = "_height_timestamp.pkl"

    def __init__(self, chain: ChainType, save_path: str):
        self.height_cache_path = os.path.join(save_path, chain.value + HeightCacheManager.height_cache_file_name)
        if os.path.exists(self.height_cache_path):
            with open(self.height_cache_path, 'rb') as f:
                self.block_dict = pickle.load(f)
                utils.print_log(f"Height cache has loaded, length: {len(self.block_dict)}")
        else:
            self.block_dict: Dict[int, datetime] = {}
            utils.print_log("Can not find a height cache, will generate one")

    def has(self, height: int):
        return height in self.block_dict

    def get(self, height: int):
        if height in self.block_dict:
            return self.block_dict[height]
        else:
            return None

    def set(self, height: int, timestamp: datetime):
        self.block_dict[height] = timestamp

    def save(self):
        with open(self.height_cache_path, 'wb') as f:
            pickle.dump(self.block_dict, f)
        utils.print_log(f"Save block timestamp cache to {self.height_cache_path}, length: {len(self.block_dict)}")


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
                            http_proxy: str | None = None):
    # 从start and end 时间获取高度
    if start_height is None and end_height is None:
        if start is None and end is None:
            raise RuntimeError("Ether fill start/end date or start end height")
        start_height = query_blockno_from_time(chain, datetime.combine(start, datetime.min.time()), False, http_proxy)
        utils.print_log("Querying end timestamp, wait for 5 seconds to prevent max rate limit")
        time.sleep(5.2)  # to prevent request limit
        end_height = query_blockno_from_time(chain, datetime.combine(end, datetime.max.time()), True, http_proxy)

    # 通过eth rpc下载, 并获得按照高度划分的event临时文件列表
    client = EthRpcClient(end_point, http_proxy, auth_string)
    utils.print_log(f"Will download from height {start_height} to {end_height}")
    try:
        tmp_files_path: List[str] = \
            query_event_by_height(chain,
                                  client,
                                  ContractConfig(pool_addr, [constants.SWAP_KECCAK, constants.BURN_KECCAK, constants.COLLECT_KECCAK, constants.MINT_KECCAK]),
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
    for tmp_file in tmp_files_path:
        logs: List[Dict] = _load_tmp_file(tmp_file)
        for log in logs:
            log_day = log["block_dt"].date()
            if not current_day:
                current_day = log_day
            if log_day != current_day:  # save current day logs to file
                raw_file_list.append(_save_one_day(save_path, current_day, pool_addr, current_day_logs, chain))
                current_day_logs = []
            del log["block_dt"]  # append to write
            log["pool_tx_index"] = log.pop("transaction_index")
            log["pool_log_index"] = log.pop("log_index")
            log["pool_topics"] = log.pop("topics")
            log["pool_data"] = log.pop("data")
            current_day_logs.append(log)

    # save rest to file
    raw_file_list.append(_save_one_day(save_path, current_day, pool_addr, current_day_logs, chain))
    # 删除临时文件
    # [os.remove(f) for f in tmp_files_path]
    return raw_file_list


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


def query_event_by_height(chain: ChainType,
                          client: EthRpcClient,
                          contract_config: ContractConfig,
                          start_height: int,
                          end_height: int,
                          height_cache: HeightCacheManager = None,
                          save_path: str = "./",
                          save_every_query: int = 10,
                          batch_size: int = 500,
                          one_by_one: bool = False) -> List[str]:
    """
    根据输入参数, 下载对应高度的log,
    log会按照高度划分, 保存为临时文件.

    :param chain: chain
    :type chain:
    :param client: eth rpc client
    :type client:
    :param contract_config: 配置信息
    :type contract_config:
    :param start_height: 开始高度
    :type start_height:
    :param end_height: 结束高度
    :type end_height:
    :param height_cache: 高度-时间戳 缓存的路径
    :type height_cache:
    :param save_path: 临时文件保存路径
    :type save_path:
    :param save_every_query: 多少次查询会保存一次临时文件. save_every_query * batch_size = 临时文件里的数据条数.
    :type save_every_query:
    :param batch_size: 一次下载多少个block的log
    :type batch_size:
    :param one_by_one: 逐个下载每一个topic, 还是下载所有的topic再筛选掉不需要的.
    :type one_by_one:
    :return: 临时文件的文件名
    :rtype:
    """

    collect_dt, logs_to_save, collect_start = None, [], None  # collect date, date log by day，collect start time
    tmp_file_full_pathes = []
    if not height_cache:
        height_cache = HeightCacheManager(chain, save_path)
    batch_count = start_blk = end_blk = 0
    skip_until = -1
    with tqdm(total=(end_height - start_height + 1), ncols=150) as pbar:
        for height_slice in _cut([i for i in range(start_height, end_height + 1)], batch_size):
            start = height_slice[0]
            end = height_slice[-1]
            if end <= skip_until:
                batch_count += 1
                pbar.update(n=len(height_slice))
                continue
            if batch_count % save_every_query == 0:
                start_blk = start
                tmp_end_blk = start + batch_size * save_every_query - 1
                if tmp_end_blk >= end_height:
                    tmp_end_blk = end_height
                # 下载之前检测文件是否已经存在, 如果存在跳过下载
                tmp_file_path = _get_tmp_file_path(save_path, start_blk, tmp_end_blk, chain, contract_config.address)
                if os.path.exists(tmp_file_path):
                    skip_until = tmp_end_blk
                    batch_count += 1
                    pbar.update(n=len(height_slice))
                    tmp_file_full_pathes.append(tmp_file_path)
                    continue
            # 下载之前检测文件是否已经存在, 如果存在跳过下载

            if one_by_one:
                logs = []
                for topic_hex in contract_config.topics:
                    tmp_logs = client.get_logs(GetLogsParam(contract_config.address, start, end, [topic_hex]))
                    logs.extend(tmp_logs)
            else:
                logs = client.get_logs(GetLogsParam(contract_config.address, start, end, None))
            log_lst = []
            for log in logs:
                log['blockNumber'] = int(log['blockNumber'], 16)
                if len(log['topics']) > 0 and (log['topics'][0] in contract_config.topics):
                    if log["removed"]:
                        continue
                    log_lst.append({
                        'block_number': log['blockNumber'],
                        'transaction_hash': log['transactionHash'],
                        'transaction_index': log['transactionIndex'],
                        'log_index': log['logIndex'],
                        'data': log["data"],
                        'topics': json.dumps(log['topics'])
                    })
            with ThreadPoolExecutor(max_workers=10) as t:
                obj_lst = []
                for data in log_lst:
                    obj = t.submit(_fill_block_info, data, client, height_cache)
                    obj_lst.append(obj)
                for future in as_completed(obj_lst):
                    data = future.result()
                    logs_to_save.append(data)

            # if got enough blocks, save file
            batch_count += 1
            if batch_count % save_every_query == 0:
                # save tmp file
                logs_to_save = sorted(logs_to_save, key=itemgetter('block_number', 'transaction_index', 'log_index'))
                end_blk = end
                tmp_file_full_pathes.append(_save_tmp_file(save_path, logs_to_save, start_blk, end_blk, chain, contract_config.address))
                logs_to_save = []
            pbar.update(n=len(height_slice))
    if batch_count % save_every_query != 0:  # save tail queries
        logs_to_save = sorted(logs_to_save, key=itemgetter('block_number', 'transaction_index', 'log_index'))
        end_blk = end
        tmp_file_full_pathes.append(_save_tmp_file(save_path, logs_to_save, start_blk, end_blk, chain, contract_config.address))
    height_cache.save()
    return tmp_file_full_pathes


def _load_tmp_file(full_path) -> List:
    with open(full_path, 'rb') as f:
        data = pickle.load(f)
    return data


def _get_tmp_file_path(save_path, start, end, chain, address):
    return os.path.join(save_path, f"{chain.name}-{address}-{start}-{end}.tmp.pkl")


def _save_tmp_file(save_path, logs, start, end, chain, address):
    file_path = _get_tmp_file_path(save_path, start, end, chain, address)
    with open(file_path, 'wb') as f:
        pickle.dump(logs, f)
        # writer = csv.DictWriter(csvfile, fieldnames=['block_number', 'block_timestamp', 'transaction_hash', 'transaction_index', 'log_index', 'data', 'topics'])
        # writer.writeheader()
        # for item in logs:
        #     writer.writerow(item)
    return file_path


def _cut(obj, sec):
    return [obj[i:i + sec] for i in range(0, len(obj), sec)]


def _fill_block_info(log, client: EthRpcClient, block_dict: HeightCacheManager):
    height = log['block_number']
    if not block_dict.has(height):
        block_dt = client.get_block_timestamp(height)
        block_dict.set(height, block_dt)
    log['block_timestamp'] = block_dict.get(height).isoformat()
    log['block_dt'] = block_dict.get(height)
    log["log_index"] = int(log["log_index"], 16)
    log["transaction_index"] = int(log["transaction_index"], 16)
    return log
