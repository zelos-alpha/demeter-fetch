import os
import unittest

import toml

from demeter_fetch import (
    ToType,
)
from demeter_fetch.core import download_by_config
from tests.utils import validate_files_by_md5


class AaveFullDownloadTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(AaveFullDownloadTest, self).__init__(*args, **kwargs)
        self.config = toml.load("config.toml")
        self.check_existed_result()
        self.base_config = None

    def check_existed_result(self):
        existed_files = os.listdir(self.config["to_path"])
        existed_files = list(filter(lambda f: f.endswith(".csv"), existed_files))
        if len(existed_files) > 0:
            is_del = input("Old result detects, delete them? y/n, default is n: ")
            if is_del == "y":
                [os.remove(os.path.join(self.config["uni_to_path"], f)) for f in existed_files]

    def test_raw(self):
        config = self.base_config
        generated_files = download_by_config(config)
        validate_files_by_md5(generated_files)

    def test_minute(self):
        config = self.base_config
        config.to_config.type = ToType.minute
        generated_files = download_by_config(config)
        validate_files_by_md5(generated_files)

    def test_tick(self):
        config = self.base_config
        config.to_config.type = ToType.tick
        generated_files = download_by_config(config)
        validate_files_by_md5(generated_files)
