import unittest
from datetime import datetime
import sys

from demeter_fetch import constants
from demeter_fetch.source_rpc import query_blockno_from_time, query_event_by_height, ContractConfig, HeightCacheManager
import demeter_fetch._typing as typing
from demeter_fetch.eth_rpc_client import EthRpcClient
from .config import end_point


class UniLpDataTest(unittest.TestCase):
    # ==========lines=========================
    def test_query_blockno_from_time(self):
        value = query_blockno_from_time(typing.ChainType.ethereum, datetime(2023, 5, 8), True, "127.0.0.1:7890")
        self.assertTrue(value == 17209709, "height not right")

    def test_query_event_by_height(self):
        client = EthRpcClient(end_point, "127.0.0.1:7890")
        height_cache = HeightCacheManager(typing.ChainType.polygon, "./sample-data")
        files = query_event_by_height(chain=typing.ChainType.polygon,
                                      client=client,
                                      contract_config=ContractConfig("0x45dda9cb7c25131df268515131f647d726f50608",
                                                                     [constants.SWAP_KECCAK, constants.BURN_KECCAK, constants.COLLECT_KECCAK, constants.MINT_KECCAK]),
                                      start_height=42447801,
                                      end_height=42448800,
                                      height_cache=height_cache,
                                      save_path="./sample-data",
                                      save_every_query=2,
                                      batch_size=500)
        height_cache.save()
        print(files)
        self.assertTrue(len(files) == 1)

    def test_query_event_by_height_save_rest(self):
        client = EthRpcClient(end_point, "127.0.0.1:7890")
        height_cache = HeightCacheManager(typing.ChainType.polygon, "./sample-data")
        files = query_event_by_height(chain=typing.ChainType.polygon,
                                      client=client,
                                      contract_config=ContractConfig("0x45dda9cb7c25131df268515131f647d726f50608",
                                                                     [constants.SWAP_KECCAK, constants.BURN_KECCAK, constants.COLLECT_KECCAK, constants.MINT_KECCAK]),
                                      start_height=42447301,  # height difference can not be divided by 2*500
                                      end_height=42448800,
                                      height_cache=height_cache,
                                      save_path="./sample-data",
                                      save_every_query=2,
                                      batch_size=500)
        height_cache.save()
        print(files)
        self.assertTrue(len(files) == 2)
