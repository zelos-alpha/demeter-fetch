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
)
from demeter_fetch.core import download_by_config
from .utils import validate_files_by_md5


class FullDownloadBigQueryTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(FullDownloadBigQueryTest, self).__init__(*args, **kwargs)
        self.config = toml.load("./config.toml")
        self.existing_files= []

    def test_raw(self):
        config = Config(
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
        generated_files = download_by_config(config)
        self.existing_files.extend(generated_files)
        validate_files_by_md5(generated_files)
