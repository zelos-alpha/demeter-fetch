from tests.test_aave_full import AaveFullDownloadTest

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
    AaveConfig,
)


class FullDownloadBigQueryTest(AaveFullDownloadTest):
    def __init__(self, *args, **kwargs):
        super(FullDownloadBigQueryTest, self).__init__(*args, **kwargs)
        self.base_config = Config(
            FromConfig(
                chain=ChainType.polygon,
                data_source=DataSource.big_query,
                dapp_type=DappType.aave,
                start=date(2024, 1, 6),
                end=date(2024, 1, 6),
                aave_config=AaveConfig(
                    tokens=["0x7ceb23fd6bc0add59e62ac25578270cff1b9f619", "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"],
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

    def test_tick(self):
        super().test_tick()
