import unittest
from datetime import date
import os
import toml
import hashlib
import pandas as pd

from demeter_fetch import (
    Config,
    ToConfig,
    FromConfig,
    ChainType,
    DataSource,
    DappType,
    UniswapConfig,
    BigQueryConfig,
    ToType,
    ToFileType,
    RpcConfig,
)
from demeter_fetch.core import download_by_config
from tests.test_uni_full import FullDownloadTest
from tests.utils import validate_files_by_md5


class FullDownloadRPCTest(FullDownloadTest):

    def __init__(self, *args, **kwargs):
        super(FullDownloadRPCTest, self).__init__(*args, **kwargs)
        self.base_config = Config(
            FromConfig(
                chain=ChainType.ethereum,
                data_source=DataSource.rpc,
                dapp_type=DappType.uniswap,
                start=date(2024, 1, 5),
                end=date(2024, 1, 5),
                uniswap_config=UniswapConfig(
                    pool_address="0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                    ignore_position_id=True,
                ),
                rpc=RpcConfig(
                    end_point=self.config["end_point"],
                    auth_string=self.config["auth"] if "auth" in self.config else None,
                    keep_tmp_files=False,
                    etherscan_api_key=self.config["etherscan_api_key"],
                    force_no_proxy=True,  # if set to true, will ignore proxy setting
                ),
                http_proxy=self.config["http_proxy"],
            ),
            ToConfig(
                type=ToType.raw,
                save_path=self.config["to_path"],
                skip_existed=True,
                to_file_type=ToFileType.csv,
                keep_raw=True,
                multi_process=False,
            ),
        )

    def test_raw(self):
        super().test_raw()

    def test_minute(self):
        super().test_minute()

    def test_tick_no_pos(self):
        super().test_tick_no_pos()

    def test_tick(self):
        super().test_tick()

    def test_position(self):
        super().test_position()

    def test_user_lp(self):
        super().test_user_lp()

    def test_price(self):
        super().test_price()
