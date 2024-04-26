from datetime import date

from demeter_fetch import (
    Config,
    ToConfig,
    FromConfig,
    ChainType,
    DataSource,
    DappType,
    ToType,
    ToFileType,
    RpcConfig,
)
from tests.test_squeeth_full import FullDownloadTest


class FullDownloadRPCTest(FullDownloadTest):

    def __init__(self, *args, **kwargs):
        super(FullDownloadRPCTest, self).__init__(*args, **kwargs)
        self.base_config = Config(
            FromConfig(
                chain=ChainType.ethereum,
                data_source=DataSource.rpc,
                dapp_type=DappType.squeeth,
                start=date(2024, 1, 5),
                end=date(2024, 1, 5),
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

