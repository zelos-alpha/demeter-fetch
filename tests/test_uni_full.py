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
    TokenConfig,
)
from demeter_fetch.core import download_by_config
from tests.utils import validate_files_by_md5


class FullDownloadTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(FullDownloadTest, self).__init__(*args, **kwargs)
        self.config = toml.load("config.toml")
        self.check_existed_result()
        self.base_config = None

    def check_existed_result(self):
        existed_files = os.listdir(self.config["to_path"])
        existed_files = list(filter(lambda f: f.endswith(".csv"), existed_files))
        if len(existed_files) > 0:
            is_del = input("Old result detects, delete them? y/n, default is n: ")
            if is_del == "y":
                [os.remove(os.path.join(self.config["to_path"], f)) for f in existed_files]

    def test_raw(self):
        config = self.base_config
        generated_files = download_by_config(config)
        validate_files_by_md5(generated_files)

    def test_minute(self):
        config = self.base_config
        config.to_config.type = ToType.minute
        generated_files = download_by_config(config)
        validate_files_by_md5(generated_files)

    def test_tick_no_pos(self):
        config = self.base_config
        config.to_config.type = ToType.tick
        generated_files = download_by_config(config)
        validate_files_by_md5(generated_files)

    def test_tick(self):
        config = self.base_config
        config.to_config.type = ToType.tick
        config.from_config.uniswap_config.ignore_position_id = False
        generated_files = download_by_config(config)
        validate_files_by_md5(generated_files)

    def test_position(self):
        config = self.base_config
        config.to_config.type = ToType.position
        generated_files = download_by_config(config)
        validate_files_by_md5(generated_files)

    def test_user_lp(self):
        config = self.base_config
        config.to_config.type = ToType.user_lp
        generated_files = download_by_config(config)
        validate_files_by_md5(generated_files)

    def test_price(self):
        config = self.base_config
        config.from_config.uniswap_config = UniswapConfig(
            pool_address="0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
            ignore_position_id=True,
            token0=TokenConfig(name="usdc", decimal=6),
            token1=TokenConfig(name="eth", decimal=18),
            is_token0_base=True,
        )

        config.to_config.type = ToType.price
        generated_files = download_by_config(config)
        validate_files_by_md5(generated_files)

