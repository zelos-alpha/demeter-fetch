import os.path
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import pickle
import random
import requests
from dataclasses import dataclass
from datetime import datetime, UTC, timezone
from operator import itemgetter
from sqlitedict import SqliteDict
from tqdm import tqdm  # process bar
from typing import List, Dict

import demeter_fetch.common.utils as utils
from demeter_fetch import ChainType, EthError
from .source_utils import ContractConfig


@dataclass
class GetLogsParam:
    address: str
    fromBlock: int
    toBlock: int
    topics: List[str] | None


class EthRpcClient:
    def __init__(self, endpoint: str, proxy="", auth=""):
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=5, pool_maxsize=20)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.headers = {}
        self.endpoint = endpoint
        if auth:
            self.headers["Authorization"] = auth
        self.proxies = (
            {
                "http": proxy,
                "https": proxy,
            }
            if proxy
            else {}
        )

    def __del__(self):
        self.session.close()

    @staticmethod
    def __encode_json_rpc(method: str, params: list):
        return {"jsonrpc": "2.0", "method": method, "params": params, "id": random.randint(1, 2147483648)}

    @staticmethod
    def __decode_json_rpc(response: requests.Response):
        try:
            content = response.json()
        except Exception as e:
            print(f"Decode rpc response failed, error: {e}")
            raise e

        if "error" in content:
            raise EthError(content["error"]["code"], content["error"]["message"])
        return content["result"]

    def do_post(self, param):
        return self.session.post(self.endpoint, json=param, proxies=self.proxies, headers=self.headers)

    def get_block(self, height):
        return self.send("eth_getBlockByNumber", [hex(height), False])

    def get_block_timestamp(self, height):
        resp = self.get_block(height)
        if resp:
            timestamp = int(resp["timestamp"], 16)
            return datetime.fromtimestamp(timestamp, UTC)
        else:
            return None

    def get_tx_receipt(self, tx_hash):
        return self.send("eth_getTransactionReceipt", [tx_hash])

    def get_tx(self, tx_hash):
        """
        response:
            "hash":"0xc6ef2fc5426d6ad6fd9e2a26abeab0aa2411b7ab17f30a99d3cb96aed1d1055b",
            "nonce":"0x",
            "blockHash": "0xbeab0aa2411b7ab17f30a99d3cb9c6ef2fc5426d6ad6fd9e2a26a6aed1d1055b",
            "blockNumber": "0x15df", // 5599
            "transactionIndex":  "0x1", // 1
            "from":"0x407d73d8a49eeb85d32cf465507dd71d507100c1",
            "to":"0x85h43d8a49eeb85d32cf465507dd71d507100c1",
            "value":"0x7f110", // 520464
            "gas": "0x7f110", // 520464
            "gasPrice":"0x09184e72a000",
            "input":"0x603880600c6000396000f300603880600c6000396000f3603880600c6000396000f360",
        """
        return self.send("eth_getTransactionByHash", [tx_hash])

    def get_logs(self, param: GetLogsParam):
        if param.toBlock:
            param.toBlock = hex(param.toBlock)
        if param.fromBlock:
            param.fromBlock = hex(param.fromBlock)
        return self.send("eth_getLogs", [vars(param)])

    def send(self, commend: str, params: List):
        response = self.do_post(EthRpcClient.__encode_json_rpc(commend, params))
        if response.status_code != 200:
            raise RuntimeError("Request rpc return error with code {}".format(response.status_code))
        return EthRpcClient.__decode_json_rpc(response)


class CacheEngineType:
    sqlite = 1
    leveldb = 2
    dict_pickle = 3


class HeightCacheManager:
    """
    height => block_timestamp cache

    There are 3 storage engine:

    pkl: old format.

    sqlitedict: default format

    levelDB: fast and take less storage. But it's difficult to install in windows
    """

    sqlite_file_name = "_height_timestamp.sqlite"
    leveldb_file_name = "_height_timestamp_levelDB"
    pkl_file_name = "_height_timestamp.pkl"

    def __init__(self, chain: ChainType, save_path: str):
        sqlite_cache_path = os.path.join(save_path, chain.name + HeightCacheManager.sqlite_file_name)
        level_cache_path = os.path.join(save_path, chain.name + HeightCacheManager.leveldb_file_name)
        pkl_cache_path = os.path.join(save_path, chain.name + HeightCacheManager.pkl_file_name)
        if os.path.exists(level_cache_path):
            self.cache_engine = CacheEngineType.leveldb
            self.height_cache_path = level_cache_path
            # do not import plyvel unless required. in windows install plyvel is too complex(need visual studio c++)
            import plyvel

            self._block_dict = plyvel.DB(self.height_cache_path, create_if_missing=True)
        elif os.path.exists(pkl_cache_path):
            self.height_cache_path = pkl_cache_path
            self.cache_engine = CacheEngineType.dict_pickle
            with open(self.height_cache_path, "rb") as f:
                self.block_dict = pickle.load(f)
                utils.print_log(f"Height cache has loaded, length: {len(self.block_dict)}")
        else:  # will use and create a sqlite instance by default
            self.cache_engine = CacheEngineType.sqlite
            self.height_cache_path = sqlite_cache_path
            self._block_dict = SqliteDict(self.height_cache_path, outer_stack=False)  # True is the default
            utils.print_log(f"Height cache has loaded, length: {len(self._block_dict)}")
        print("Height cache path: " + str(self.height_cache_path))
        self.in_mem_count = 0

    def __contains__(self, item) -> bool:
        if self.cache_engine == CacheEngineType.sqlite:
            return item in self._block_dict
        elif self.cache_engine == CacheEngineType.leveldb:
            return self._block_dict.get(item.to_bytes(4)) is None
        elif self.cache_engine == CacheEngineType.dict_pickle:
            return item in self.block_dict
        else:
            return False

    def get(self, height: int) -> datetime | None:
        if self.cache_engine == CacheEngineType.sqlite:
            return self._block_dict[height] if height in self._block_dict else None
        elif self.cache_engine == CacheEngineType.leveldb:
            cache_val = self._block_dict.get(height.to_bytes(4))
            return (
                None if cache_val is None else datetime.fromtimestamp(int.from_bytes(cache_val) / 1000, tz=timezone.utc)
            )
        elif self.cache_engine == CacheEngineType.dict_pickle:
            return self.block_dict[height] if height in self.block_dict else None
        else:
            return None

    def set(self, height: int, timestamp: datetime):
        if self.cache_engine == CacheEngineType.sqlite:
            self._block_dict[height] = timestamp
            self.in_mem_count += 1
            if self.in_mem_count >= 1000:
                self._block_dict.commit()
                self.in_mem_count = 0
        elif self.cache_engine == CacheEngineType.leveldb:
            self._block_dict.put(height.to_bytes(4), int(timestamp.timestamp() * 1000).to_bytes(6))
        elif self.cache_engine == CacheEngineType.dict_pickle:
            self.block_dict[height] = timestamp

    def save(self):
        if self.cache_engine == CacheEngineType.sqlite:
            self._block_dict.commit()
            self.in_mem_count = 0
        elif self.cache_engine == CacheEngineType.leveldb:
            pass
        elif self.cache_engine == CacheEngineType.dict_pickle:
            with open(self.height_cache_path, "wb") as f:
                pickle.dump(self.block_dict, f)
            utils.print_log(f"Save block timestamp cache to {self.height_cache_path}, length: {len(self._block_dict)}")

    def __del__(self):
        if self.cache_engine == CacheEngineType.sqlite or self.cache_engine == CacheEngineType.leveldb:
            self._block_dict.close()

    def __len__(self):
        if self.cache_engine == CacheEngineType.sqlite:
            return len(self._block_dict)
        elif self.cache_engine == CacheEngineType.dict_pickle:
            return len(self._block_dict)
        else:
            return -1


def _query_tx_receipt(param):
    tx_hash, client = param
    resp = client.get_tx_receipt(tx_hash)
    return resp


def _query_tx(param):
    tx_hash, client = param
    resp = client.get_tx(tx_hash)
    return resp


def query_tx(client: EthRpcClient, tx_list: pd.Series, threads=10) -> pd.DataFrame:
    param_list = [(tx_hash, client) for tx_hash in tx_list]
    with ThreadPoolExecutor(max_workers=threads) as executor:
        txes = list(tqdm(executor.map(_query_tx, param_list), ncols=60, position=1, leave=False, total=len(tx_list)))

    tx_list = []
    for tx in txes:
        if tx is None:
            continue
        tx_list.append(
            {
                "transaction_hash": tx["hash"],  # "0xc6ef2fc5426d6ad6fd9e2a26abeab0aa2411b7ab17f30a99d3cb96aed1d1055b",
                "block_number": int(tx["blockNumber"], 16),
                "transaction_index": int(tx["transactionIndex"], 16),  #  "0x1",
                "from": tx["from"],  #  "0x407d73d8a49eeb85d32cf465507dd71d507100c1",
                "to": tx["to"],  #  "0x85h43d8a49eeb85d32cf465507dd71d507100c1",
                "value": int(tx["value"], 16),  #  "0x7f110",
                # "gas": int(tx["gas"], 16),  #  "0x7f110",
                # "gas_price": int(tx["gasPrice"], 16),  #  "0x09184e72a000",
            }
        )

    df = pd.DataFrame(tx_list)
    return df


def query_event_by_tx(client: EthRpcClient, tx_list: pd.Series, threads=10) -> pd.DataFrame:
    param_list = [(tx_hash, client) for tx_hash in tx_list]
    with ThreadPoolExecutor(max_workers=threads) as executor:
        tx_receipts = list(
            tqdm(executor.map(_query_tx_receipt, param_list), ncols=60, position=1, leave=False, total=len(tx_list))
        )

    logs_list = []
    for tx in tx_receipts:
        if tx is None:
            continue
        for log in tx["logs"]:
            logs_list.append(
                {
                    "block_number": int(log["blockNumber"], 16),
                    "transaction_hash": log["transactionHash"],
                    "transaction_index": int(log["transactionIndex"], 16),
                    "from": tx["from"],
                    "to": tx["to"],
                    "log_index": int(log["logIndex"], 16),
                    "log_address": log["address"],
                    "topics": log["topics"],
                    "data": log["data"] if log["data"] != "0x" else "",
                }
            )
    df = pd.DataFrame(logs_list)
    return df


def get_event_slice(client, contract_config, start, end, one_by_one):
    # allow download all when no topic is specified
    if (
        one_by_one
        and len(contract_config.topics0) > 0
        and len(contract_config.topics1) > 0
        and len(contract_config.topics2) > 0
        and len(contract_config.topics3) > 0
    ):
        logs = []
        for topic_hex in contract_config.topics0:
            if len(contract_config.topics1) > 0:
                for topic1_hex in contract_config.topics1:
                    if len(contract_config.topics2) > 0:
                        for topic2_hex in contract_config.topics2:
                            if len(contract_config.topics3) > 0:
                                for topic3_hex in contract_config.topics3:
                                    tmp_logs = client.get_logs(
                                        GetLogsParam(
                                            contract_config.address,
                                            start,
                                            end,
                                            [topic_hex, topic1_hex, topic2_hex, topic3_hex],
                                        )
                                    )
                                    logs.extend(tmp_logs)
                            else:
                                tmp_logs = client.get_logs(
                                    GetLogsParam(
                                        contract_config.address, start, end, [topic_hex, topic1_hex, topic2_hex]
                                    )
                                )
                                logs.extend(tmp_logs)
                    else:
                        tmp_logs = client.get_logs(
                            GetLogsParam(contract_config.address, start, end, [topic_hex, topic1_hex])
                        )
                        logs.extend(tmp_logs)
            else:
                tmp_logs = client.get_logs(GetLogsParam(contract_config.address, start, end, [topic_hex]))
                logs.extend(tmp_logs)

    else:
        logs = client.get_logs(GetLogsParam(contract_config.address, start, end, None))
    return logs


def _is_log_useful(log: Dict, contract_config: ContractConfig) -> bool:
    if log["removed"]:
        return False
    if len(contract_config.topics0) > 0 and log["topics"][0] not in contract_config.topics0:
        return False
    # match topic1
    if len(contract_config.topics1) > 0 and log["topics"][1] not in contract_config.topics1:
        return False
    # match topic2
    if len(contract_config.topics2) > 0 and log["topics"][2] not in contract_config.topics2:
        return False
    # match topic3
    if len(contract_config.topics3) > 0 and log["topics"][3] not in contract_config.topics3:
        return False
    return True


def query_event_by_height_concurrent(
    chain: ChainType,
    client: EthRpcClient,
    contract_config: ContractConfig,
    start_height: int,
    end_height: int,
    height_cache: HeightCacheManager = None,
    height_cache_path: str = None,
    save_path: str = "./",
    batch_size: int = 500,
    one_by_one: bool = False,
    skip_timestamp: bool = False,
    thread: int = 10,
) -> List[str]:
    """



    :param chain:
    :param client:
    :param contract_config:
    :param start_height:
    :param end_height:
    :param height_cache:
    :param height_cache_path:
    :param save_path:
    :param batch_size:
    :param one_by_one: query every log in contract_config one by one, or download all logs then filter with contract_config
    :param skip_timestamp:
    :param thread:
    :return:
    """
    tmp_file_path = get_tmp_file_path(save_path, start_height, end_height, chain, contract_config.address)
    if os.path.exists(tmp_file_path):
        return [tmp_file_path]
    if not height_cache:
        height_cache = HeightCacheManager(chain, save_path if height_cache_path is None else height_cache_path)
    utils.print_log(f"Querying {contract_config.address} from {start_height} to {end_height}")
    task_list = [start_height + i * batch_size for i in range((end_height - start_height) // batch_size + 1)]
    raw_log_list = []
    with ThreadPoolExecutor(max_workers=thread) as t:
        async_list = []
        for start in task_list:
            end = start + batch_size - 1
            if end > end_height:
                end = end_height
            obj = t.submit(get_event_slice, client, contract_config, start, end, one_by_one)
            async_list.append(obj)
        for future in tqdm(
            as_completed(async_list), total=len(async_list), position=1, leave=False, desc="Loading logs"
        ):
            data = future.result()
            raw_log_list.extend(data)
    log_list = []
    for log in raw_log_list:
        if not _is_log_useful(log, contract_config):
            continue
        log["blockNumber"] = int(log["blockNumber"], 16)

        # block_number, block_timestamp, transaction_hash, transaction_index, log_index, topics, data
        log_list.append(
            {
                "block_number": log["blockNumber"],
                "transaction_hash": log["transactionHash"],
                "transaction_index": log["transactionIndex"],
                "log_index": log["logIndex"],
                "data": log["data"],
                "topics": log["topics"],
            }
        )
    for log in log_list:
        log["log_index"] = int(log["log_index"], 16)
        log["transaction_index"] = int(log["transaction_index"], 16)

    if not skip_timestamp:
        with ThreadPoolExecutor(max_workers=thread) as t:
            async_list = []
            for data in log_list:
                obj = t.submit(_fill_block_info, data, client, height_cache)
                async_list.append(obj)
            for future in tqdm(
                as_completed(async_list), total=len(async_list), position=1, leave=False, desc="Appending timestamp"
            ):
                future.result()
    height_cache.save()
    return [save_tmp_file(save_path, log_list, start_height, end_height, chain, contract_config.address)]


def query_event_by_height(
    chain: ChainType,
    client: EthRpcClient,
    contract_config: ContractConfig,
    start_height: int,
    end_height: int,
    height_cache: HeightCacheManager = None,
    height_cache_path: str = None,
    save_path: str = "./",
    save_every_query: int = 10,
    batch_size: int = 500,
    one_by_one: bool = False,
    skip_timestamp: bool = False,
) -> List[str]:
    """
    根据输入参数, 下载对应高度的log,
    log会按照高度划分, 保存为临时文件.

    :param chain: chain
    :param client: eth rpc client
    :param contract_config: 配置信息
    :param start_height: 开始高度
    :param end_height: 结束高度
    :param height_cache: 高度-时间戳 缓存的路径
    :param save_path: 临时文件保存路径
    :param save_every_query: 多少次查询会保存一次临时文件. save_every_query * batch_size = 临时文件里的数据条数.
    :param batch_size: 一次下载多少个block的log
    :param one_by_one: 逐个下载每一个topic, 还是下载所有的topic再筛选掉不需要的.
    :param skip_timestamp: skip query block timestamp
    :return: 临时文件的文件名
    :rtype:
    """
    warnings.warn("use query_event_by_height_concurrent", DeprecationWarning)
    collect_dt, logs_to_save, collect_start = None, [], None  # collect date, date log by day，collect start time
    tmp_file_full_path_list = []
    if not height_cache:
        height_cache = HeightCacheManager(chain, save_path if height_cache_path is None else height_cache_path)
    batch_count = start_blk = end_blk = 0
    skip_until = -1
    utils.print_log(f"Querying {contract_config.address} from {start_height} to {end_height}")
    with tqdm(total=(end_height - start_height + 1), ncols=60, position=1, leave=False) as pbar:
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
                tmp_file_path = get_tmp_file_path(save_path, start_blk, tmp_end_blk, chain, contract_config.address)
                if os.path.exists(tmp_file_path):
                    skip_until = tmp_end_blk
                    batch_count += 1
                    pbar.update(n=len(height_slice))
                    tmp_file_full_path_list.append(tmp_file_path)
                    continue
            # 下载之前检测文件是否已经存在, 如果存在跳过下载

            if one_by_one:
                logs = []
                for topic_hex in contract_config.topics0:
                    for topic1_hex in contract_config.topics1:
                        tmp_logs = client.get_logs(
                            GetLogsParam(contract_config.address, start, end, [topic_hex, topic1_hex])
                        )
                    logs.extend(tmp_logs)
            else:
                logs = client.get_logs(GetLogsParam(contract_config.address, start, end, None))
            log_list = []
            for log in logs:
                log["blockNumber"] = int(log["blockNumber"], 16)
                if len(log["topics"]) > 0 and (log["topics"][0] in contract_config.topics0):
                    if not _is_log_useful(log, contract_config):
                        continue
                    # block_number, block_timestamp, transaction_hash, transaction_index, log_index, topics, data
                    log_list.append(
                        {
                            "block_number": log["blockNumber"],
                            "transaction_hash": log["transactionHash"],
                            "transaction_index": log["transactionIndex"],
                            "log_index": log["logIndex"],
                            "data": log["data"],
                            "topics": log["topics"],
                        }
                    )
            for log in log_list:
                log["log_index"] = int(log["log_index"], 16)
                log["transaction_index"] = int(log["transaction_index"], 16)
            if skip_timestamp:
                logs_to_save.extend(log_list)
            else:
                with ThreadPoolExecutor(max_workers=10) as t:
                    obj_lst = []
                    for data in log_list:
                        obj = t.submit(_fill_block_info, data, client, height_cache)
                        obj_lst.append(obj)
                    for future in as_completed(obj_lst):
                        data = future.result()
                        logs_to_save.append(data)

            # if got enough blocks, save file
            batch_count += 1
            if batch_count % save_every_query == 0:
                # save tmp file
                logs_to_save = sorted(logs_to_save, key=itemgetter("block_number", "transaction_index", "log_index"))
                end_blk = end
                tmp_file_full_path_list.append(
                    save_tmp_file(save_path, logs_to_save, start_blk, end_blk, chain, contract_config.address)
                )
                logs_to_save = []
            pbar.update(n=len(height_slice))
    if batch_count % save_every_query != 0 and skip_until < end:  # save tail queries
        logs_to_save = sorted(logs_to_save, key=itemgetter("block_number", "transaction_index", "log_index"))
        end_blk = end
        tmp_file_full_path_list.append(
            save_tmp_file(save_path, logs_to_save, start_blk, end_blk, chain, contract_config.address)
        )
    height_cache.save()
    return tmp_file_full_path_list


def load_tmp_file(full_path) -> List:
    with open(full_path, "rb") as f:
        data = pickle.load(f)
    return data


def get_tmp_file_path(save_path, start, end, chain, address):
    return os.path.join(save_path, f"{chain.name}-{address}-{start}-{end}.tmp.pkl")


def save_tmp_file(save_path, logs, start, end, chain, address):
    file_path = get_tmp_file_path(save_path, start, end, chain, address)
    with open(file_path, "wb") as f:
        pickle.dump(logs, f)
    return file_path


def _cut(obj, sec):
    return [obj[i : i + sec] for i in range(0, len(obj), sec)]


def _fill_block_info(log, client: EthRpcClient, cache_manager: HeightCacheManager):
    height = log["block_number"]
    if height not in cache_manager:
        block_dt = client.get_block_timestamp(height)
        cache_manager.set(height, block_dt)
    log["block_timestamp"] = cache_manager.get(height).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    log["block_dt"] = cache_manager.get(height)


def set_position_id(row: pd.Series) -> str:
    if (row["position_id"] is not None) and (not np.isnan(row["position_id"])):
        return str(int(row["position_id"]))
    return f"{row['owner']}-{int(row['tick_lower'])}-{int(row['tick_upper'])}"
