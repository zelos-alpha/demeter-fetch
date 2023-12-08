import json
import os.path
import pickle
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from operator import itemgetter
from typing import List, Dict

import requests
from tqdm import tqdm  # process bar

import demeter_fetch.utils as utils
from demeter_fetch._typing import ChainType
from demeter_fetch._typing import EthError


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
        content = response.json()
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
            return datetime.utcfromtimestamp(timestamp)
        else:
            return None

    def get_logs(self, param: GetLogsParam):
        if param.toBlock:
            param.toBlock = hex(param.toBlock)
        if param.fromBlock:
            param.fromBlock = hex(param.fromBlock)
        return self.send("eth_getLogs", [vars(param)])

    def send(self, commend: str, params: List):
        response = self.do_post(EthRpcClient.__encode_json_rpc(commend, params))
        return EthRpcClient.__decode_json_rpc(response)


class HeightCacheManager:
    """
    高度缓存
    """

    height_cache_file_name = "_height_timestamp.pkl"

    def __init__(self, chain: ChainType, save_path: str):
        self.height_cache_path = os.path.join(save_path, chain.value + HeightCacheManager.height_cache_file_name)
        if os.path.exists(self.height_cache_path):
            with open(self.height_cache_path, "rb") as f:
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
        with open(self.height_cache_path, "wb") as f:
            pickle.dump(self.block_dict, f)
        utils.print_log(f"Save block timestamp cache to {self.height_cache_path}, length: {len(self.block_dict)}")


@dataclass
class ContractConfig:
    address: str
    topics: List[str]


def query_event_by_height(
    chain: ChainType,
    client: EthRpcClient,
    contract_config: ContractConfig,
    start_height: int,
    end_height: int,
    height_cache: HeightCacheManager = None,
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

    collect_dt, logs_to_save, collect_start = None, [], None  # collect date, date log by day，collect start time
    tmp_file_full_path_list = []
    if not height_cache:
        height_cache = HeightCacheManager(chain, save_path)
    batch_count = start_blk = end_blk = 0
    skip_until = -1
    with tqdm(total=(end_height - start_height + 1), ncols=120) as pbar:
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
                for topic_hex in contract_config.topics:
                    tmp_logs = client.get_logs(GetLogsParam(contract_config.address, start, end, [topic_hex]))
                    logs.extend(tmp_logs)
            else:
                logs = client.get_logs(GetLogsParam(contract_config.address, start, end, None))
            log_list = []
            for log in logs:
                log["blockNumber"] = int(log["blockNumber"], 16)
                if len(log["topics"]) > 0 and (log["topics"][0] in contract_config.topics):
                    if log["removed"]:
                        continue
                    log_list.append(
                        {
                            "block_number": log["blockNumber"],
                            "transaction_hash": log["transactionHash"],
                            "transaction_index": log["transactionIndex"],
                            "log_index": log["logIndex"],
                            "data": log["data"],
                            "topics": json.dumps(log["topics"]),
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
                tmp_file_full_path_list.append(save_tmp_file(save_path, logs_to_save, start_blk, end_blk, chain, contract_config.address))
                logs_to_save = []
            pbar.update(n=len(height_slice))
    if batch_count % save_every_query != 0 and skip_until < end:  # save tail queries
        logs_to_save = sorted(logs_to_save, key=itemgetter("block_number", "transaction_index", "log_index"))
        end_blk = end
        tmp_file_full_path_list.append(save_tmp_file(save_path, logs_to_save, start_blk, end_blk, chain, contract_config.address))
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


def _fill_block_info(log, client: EthRpcClient, block_dict: HeightCacheManager):
    height = log["block_number"]
    if not block_dict.has(height):
        block_dt = client.get_block_timestamp(height)
        block_dict.set(height, block_dt)
    log["block_timestamp"] = block_dict.get(height).isoformat()
    log["block_dt"] = block_dict.get(height)

    return log
