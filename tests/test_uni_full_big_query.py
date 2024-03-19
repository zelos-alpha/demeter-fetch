from datetime import date

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
from tests.test_uni_full import FullDownloadTest


class FullDownloadBigQueryTest(FullDownloadTest):
    def __init__(self, *args, **kwargs):
        super(FullDownloadBigQueryTest, self).__init__(*args, **kwargs)
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
