import os
import unittest
from datetime import datetime

import pandas as pd
import toml

import demeter_fetch.common._typing as typing
import demeter_fetch.sources.rpc_utils as rpc
from demeter_fetch.common import utils
from demeter_fetch.sources.source_utils import ContractConfig


class UniLpDataTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        self.config = toml.load("./config.toml")

        super(UniLpDataTest, self).__init__(*args, **kwargs)

    # ==========lines=========================
    def test_query_blockno_from_time(self):
        value = utils.ApiUtil.query_blockno_from_time(
            typing.ChainType.ethereum, datetime(2023, 5, 8), True, "127.0.0.1:7890"
        )
        self.assertTrue(value == 17212079, "height not right")

    def remove_tmp_file(self, paths: []):
        for p in paths:
            if os.path.exists(os.path.join(self.config["to_path"], p)):
                os.remove(os.path.join(self.config["to_path"], p))

    def test_query_event_by_height_concurrent(self):
        self.remove_tmp_file(
            [
                os.path.join(
                    self.config["to_path"],
                    "polygon-0x45dda9cb7c25131df268515131f647d726f50608-42447801-42448800.tmp.pkl",
                )
            ]
        )
        client = rpc.EthRpcClient(self.config["end_point"], "127.0.0.1:7890")
        height_cache = rpc.HeightCacheManager(typing.ChainType.polygon, self.config["to_path"])
        files = rpc.query_event_by_height_concurrent(
            chain=typing.ChainType.polygon,
            client=client,
            contract_config=ContractConfig(
                "0x45dda9cb7c25131df268515131f647d726f50608",
                [
                    typing.KECCAK.SWAP.value,
                    typing.KECCAK.BURN.value,
                    typing.KECCAK.COLLECT.value,
                    typing.KECCAK.MINT.value,
                ],
            ),
            start_height=42447801,
            end_height=42448800,  # diff = 999, will save in one batch
            height_cache=height_cache,
            save_path=self.config["to_path"],
            batch_size=500,
            skip_timestamp=False,
        )
        print(files)

    def test_query_event_by_height(self):
        self.remove_tmp_file(
            [
                os.path.join(
                    self.config["to_path"],
                    "polygon-0x45dda9cb7c25131df268515131f647d726f50608-42447801-42448800.tmp.pkl",
                )
            ]
        )
        client = rpc.EthRpcClient(self.config["end_point"], "127.0.0.1:7890")
        height_cache = rpc.HeightCacheManager(typing.ChainType.polygon, self.config["to_path"])
        files = rpc.query_event_by_height(
            chain=typing.ChainType.polygon,
            client=client,
            contract_config=ContractConfig(
                "0x45dda9cb7c25131df268515131f647d726f50608",
                [
                    typing.KECCAK.SWAP.value,
                    typing.KECCAK.BURN.value,
                    typing.KECCAK.COLLECT.value,
                    typing.KECCAK.MINT.value,
                ],
            ),
            start_height=42447801,
            end_height=42448800,  # diff = 999, will save in one batch
            height_cache=height_cache,
            save_path=self.config["to_path"],
            save_every_query=2,
            batch_size=500,
            skip_timestamp=True,
        )
        print(files)
        self.assertTrue(len(files) == 1)

    def test_query_event_by_height_save_rest(self):
        self.remove_tmp_file(
            [
                "polygon-0x45dda9cb7c25131df268515131f647d726f50608-42447301-42448300.tmp.pkl",
                "polygon-0x45dda9cb7c25131df268515131f647d726f50608-42448301-42448799.tmp.pkl",
            ]
        )

        client = rpc.EthRpcClient(self.config["end_point"], "127.0.0.1:7890")
        files = self.query_3_save_2(client)
        print(files)
        self.assertTrue(len(files) == 2)

    def test_query_event_by_height_save_rest_again(self):
        self.remove_tmp_file(
            [
                "polygon-0x45dda9cb7c25131df268515131f647d726f50608-42447301-42448300.tmp.pkl",
                "polygon-0x45dda9cb7c25131df268515131f647d726f50608-42448301-42448799.tmp.pkl",
            ]
        )

        client = rpc.EthRpcClient(self.config["end_point"], "127.0.0.1:7890")
        files = self.query_3_save_2(client)
        print(files)
        self.assertTrue(len(files) == 2)
        # load from existing file again
        files = self.query_3_save_2(client)
        print(files)
        self.assertTrue(len(files) == 2)

    def query_3_save_2(self, client):
        return rpc.query_event_by_height(
            chain=typing.ChainType.polygon,
            client=client,
            contract_config=ContractConfig(
                "0x45dda9cb7c25131df268515131f647d726f50608",
                [
                    typing.KECCAK.SWAP.value,
                    typing.KECCAK.BURN.value,
                    typing.KECCAK.COLLECT.value,
                    typing.KECCAK.MINT.value,
                ],
            ),
            start_height=42447301,  # height difference cannot be divided by 2*500
            end_height=42448799,
            save_path=self.config["to_path"],
            save_every_query=2,
            batch_size=500,
            skip_timestamp=True,
        )

    def test_query_event_by_height_save_rest_remove_last(self):
        client = rpc.EthRpcClient(self.config["end_point"], "127.0.0.1:7890")
        files = self.query_3_save_2(client)
        # just remove the last file.
        self.remove_tmp_file(["polygon-0x45dda9cb7c25131df268515131f647d726f50608-42448301-42448799.tmp.pkl"])
        files = self.query_3_save_2(client)
        print(files)
        self.assertTrue(len(files) == 2)

    def test_query_event_by_height_save_rest_remove_first(self):
        client = rpc.EthRpcClient(self.config["end_point"], "127.0.0.1:7890")
        files = self.query_3_save_2(client)
        # just remove the first file.
        self.remove_tmp_file(["polygon-0x45dda9cb7c25131df268515131f647d726f50608-42447301-42448300.tmp.pkl"])
        files = self.query_3_save_2(client)
        print(files)
        self.assertTrue(len(files) == 2)

    def test_query_tx_receipt(self):
        df = rpc.query_event_by_tx(
            rpc.EthRpcClient(self.config["end_point"]),
            pd.Series(
                [
                    "0xb4caa7d62ece248f8261d5e63ee76e69ed9fef0c9c72ea6cd33eae0ef9726512",
                    "0xea950a0f086d87175c4d3e7afa1299f80da9e32e2bb0460fef2aafc25e788afa",
                    "0x513de39afcc7be7cf8b3fb0391589d144c3de7c3f8f35ef0ea0317e98bc089e1",
                    "0x6e761fbdc3b0645d0f11181fd64cb1a73bc441c56716134826d67a0ad873f53d",
                    "0x20242b4d9db2d798945dcc3ddf55c3b98cc7a53c0898addd069d054f6b6867cb",
                    "0xef0419056caf269ee7b35b6ab2a8c966994206017cfd6d83a7010d888a307a75",
                    "0xeaa6ee98c343de36e96f257453a88fe2829c27ce4ef4ff0faffd9b66e5a025c1",
                    "0xeea6db2ffea4c06c665f5b902043521ea74be79fa490303fd996da6d9b1a2a71",
                    "0x06728af78607ee46346d74aa1ec967f3b7fea4b0a4c24cfb4db3bb31f2055cda",
                    "0xbfa9b92f0d91fc97c52f1986f478d282b56b3d9d898d613d0a6f5ec40122422d",
                    "0x0f92588945d6003724ce569a08854cc43334d1cf32e04c3b08f4dd1594abca81",
                    "0x3f77938b2bc9af2931652b02943935ad207548379f043890320ecbc4bfccd35d",
                    "0xe8b19fc85266a90050ddffffdd5b4d1ef57a5032cb3c1c75774d6a8f60e00682",
                    "0x8843edeacb1679d1ede456198c428634ee4089c54f934ac77eb4be5624843330",
                    "0xc6c9336989dc0a6f01cf3c976b6ffa59e8ce79213a05971a744962f43b6bc817",
                    "0xe0a25d2f322e71470545b07406fdbd867da4e3dfc51b65c55919f932f51316c7",
                    "0x92a49ecb2a738fb638afb6b9d8a88bcb99419a02224eac19c31be544600df4d4",
                    "0x9e1ab54c7a97cbbe149958e4122691eb382e7a59f0c184a258366436f4d220c8",
                    "0x0000000000000000000000000000000000000000000000000000000000000000",
                ]
            ),
            2,
        )
        self.assertTrue(len(df.index) == 182)

    def test_query_tx(self):
        df = rpc.query_tx(
            rpc.EthRpcClient(self.config["end_point"]),
            pd.Series(
                [
                    "0xb4caa7d62ece248f8261d5e63ee76e69ed9fef0c9c72ea6cd33eae0ef9726512",
                    "0xea950a0f086d87175c4d3e7afa1299f80da9e32e2bb0460fef2aafc25e788afa",
                    "0x513de39afcc7be7cf8b3fb0391589d144c3de7c3f8f35ef0ea0317e98bc089e1",
                    "0x6e761fbdc3b0645d0f11181fd64cb1a73bc441c56716134826d67a0ad873f53d",
                    "0x20242b4d9db2d798945dcc3ddf55c3b98cc7a53c0898addd069d054f6b6867cb",
                    "0xef0419056caf269ee7b35b6ab2a8c966994206017cfd6d83a7010d888a307a75",
                    "0xeaa6ee98c343de36e96f257453a88fe2829c27ce4ef4ff0faffd9b66e5a025c1",
                    "0xeea6db2ffea4c06c665f5b902043521ea74be79fa490303fd996da6d9b1a2a71",
                    "0x06728af78607ee46346d74aa1ec967f3b7fea4b0a4c24cfb4db3bb31f2055cda",
                    "0xbfa9b92f0d91fc97c52f1986f478d282b56b3d9d898d613d0a6f5ec40122422d",
                    "0x0f92588945d6003724ce569a08854cc43334d1cf32e04c3b08f4dd1594abca81",
                    "0x3f77938b2bc9af2931652b02943935ad207548379f043890320ecbc4bfccd35d",
                    "0xe8b19fc85266a90050ddffffdd5b4d1ef57a5032cb3c1c75774d6a8f60e00682",
                    "0x8843edeacb1679d1ede456198c428634ee4089c54f934ac77eb4be5624843330",
                    "0xc6c9336989dc0a6f01cf3c976b6ffa59e8ce79213a05971a744962f43b6bc817",
                    "0xe0a25d2f322e71470545b07406fdbd867da4e3dfc51b65c55919f932f51316c7",
                    "0x92a49ecb2a738fb638afb6b9d8a88bcb99419a02224eac19c31be544600df4d4",
                    "0x9e1ab54c7a97cbbe149958e4122691eb382e7a59f0c184a258366436f4d220c8",
                    "0x0000000000000000000000000000000000000000000000000000000000000000",
                ]
            ),
            2,
        )
        pass
        # self.assertTrue(len(df.index) == 182)
