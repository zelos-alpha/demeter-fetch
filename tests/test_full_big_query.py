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
    TokenConfig,
)
from demeter_fetch.core import download_by_config
from utils import validate_files_by_md5


class FullDownloadBigQueryTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(FullDownloadBigQueryTest, self).__init__(*args, **kwargs)
        self.config = toml.load("config.toml")
        self.existing_files = []
        self.base_config = Config(
            FromConfig(
                chain=ChainType.ethereum,
                data_source=DataSource.big_query,
                dapp_type=DappType.uniswap,
                start=date(2024, 1, 5),
                end=date(2024, 1, 5),
                uniswap_config=UniswapConfig(
                    pool_address="0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                    ignore_position_id=True,
                ),
                big_query=BigQueryConfig(auth_file=self.config["big_query_auth"]),
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
        config = self.base_config
        generated_files = download_by_config(config)
        self.existing_files.extend(generated_files)
        validate_files_by_md5(generated_files)

    def test_minute(self):
        config = self.base_config
        config.to_config.type = ToType.minute
        generated_files = download_by_config(config)
        self.existing_files.extend(generated_files)
        validate_files_by_md5(generated_files)

    def test_tick_no_pos(self):
        config = self.base_config
        config.to_config.type = ToType.tick
        generated_files = download_by_config(config)
        self.existing_files.extend(generated_files)
        validate_files_by_md5(generated_files)

    def test_tick(self):
        config = self.base_config
        config.to_config.type = ToType.tick
        config.from_config.uniswap_config.ignore_position_id = False
        generated_files = download_by_config(config)
        self.existing_files.extend(generated_files)
        validate_files_by_md5(generated_files)

    def test_position(self):
        config = self.base_config
        config.to_config.type = ToType.position
        generated_files = download_by_config(config)
        self.existing_files.extend(generated_files)
        validate_files_by_md5(generated_files)

    def test_user_lp(self):
        config = self.base_config
        config.to_config.type = ToType.user_lp
        generated_files = download_by_config(config)
        self.existing_files.extend(generated_files)
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
        self.existing_files.extend(generated_files)
        validate_files_by_md5(generated_files)
