import datetime
import unittest
from decimal import Decimal

import pandas as pd

from demeter_fetch import Config, FromConfig, GmxV2Config, TokenConfig
from demeter_fetch.processor_gmx2 import GmxV2PoolTx

pd.options.display.max_columns = None
pd.set_option("display.width", 5000)


class GmxV2PoolTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(GmxV2PoolTest, self).__init__(*args, **kwargs)

    def test_generate_pool(self):
        day = datetime.date(2025, 4, 7)
        node = GmxV2PoolTx()
        node.config = Config(
            FromConfig(
                None,
                None,
                None,
                None,
                None,
                gmx_v2_config=GmxV2Config(
                    GM_address="0x47c031236e19d024b42f8AE6780E44A573170703",
                    long_token=TokenConfig("BTC", 8, "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f".lower()),
                    short_token=TokenConfig("USDC", 6, "0xaf88d065e77c8cC2239327C5EDb3A432268e5831".lower()),
                    index_token=TokenConfig("BTC", 8, "0x47904963fc8b2340414262125aF798B9655E58Cd".lower()),
                ),
            ),
            None,
        )
        param = {"gmx2_tick": pd.read_feather("samples/arbitrum-GmxV2-2025-04-07.tick.feather")}
        result_df = node._process_one_day(param, day)
        print(result_df)
        self.assertEqual(len(result_df.index), 2374)
        self.assertEqual(
            Decimal(str(result_df.loc[115]["shortAmount"])) + Decimal(str(result_df.loc[115]["shortAmountDelta"])),
            Decimal(str(result_df.loc[116]["shortAmount"])),
        )
