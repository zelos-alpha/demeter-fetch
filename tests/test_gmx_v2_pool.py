import ast
import datetime
import unittest
from decimal import Decimal

import pandas as pd

from demeter_fetch import Config, FromConfig, GmxV2Config, TokenConfig
from demeter_fetch.processor_gmx2 import GmxV2PoolTx, GmxV2Minute
from demeter_fetch.processor_gmx2.pool_value_util import calcPoolValue

pd.options.display.max_columns = None
pd.set_option("display.width", 5000)

test_config = Config(
    FromConfig(
        None,
        None,
        None,
        None,
        None,
        gmx_v2_config=GmxV2Config(
            GM_address="0x47c031236e19d024b42f8AE6780E44A573170703",
            long_token=TokenConfig("WBTC", 8, "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f".lower()),
            short_token=TokenConfig("USDC", 6, "0xaf88d065e77c8cC2239327C5EDb3A432268e5831".lower()),
            index_token=TokenConfig("WBTC", 8, "0x47904963fc8b2340414262125aF798B9655E58Cd".lower()),
        ),
    ),
    None,
)


class GmxV2PoolTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(GmxV2PoolTest, self).__init__(*args, **kwargs)

    def test_generate_pool(self):
        day = datetime.date(2025, 4, 7)
        node = GmxV2PoolTx()
        node.config = test_config
        param = {"gmx2_tick": pd.read_feather("samples/arbitrum-GmxV2-2025-04-07.tick.feather")}
        result_df = node._process_one_day(param, day)
        print(result_df)
        self.assertEqual(len(result_df.index), 2374)
        self.assertEqual(
            Decimal(str(result_df.loc[115]["shortAmount"])) + Decimal(str(result_df.loc[115]["shortAmountDelta"])),
            Decimal(str(result_df.loc[116]["shortAmount"])),
        )
        result_df.to_feather(
            "./samples/arbitrum-GmxV2-0x70d95587d40a2caf56bd97485ab3eec10bee6336-2025-04-07.pool.feather"
        )

    def test_generate_minute(self):
        day = datetime.date(2025, 4, 7)
        node = GmxV2Minute()
        node.config = test_config
        node.from_config = test_config.from_config
        param = {
            "gmx2_pool": pd.read_feather(
                "samples/arbitrum-GmxV2-0x70d95587d40a2caf56bd97485ab3eec10bee6336-2025-04-07.pool.feather"
            ),
            "gmx2_price": pd.read_feather("samples/arbitrum-GmxV2-2025-04-07.price.feather"),
        }
        result_df = node._process_one_day(param, day)
        print(result_df)

    def test_compare_pool_net_value(self):
        day = datetime.date(2025, 4, 7)
        node = GmxV2Minute()
        node.config = test_config
        node.from_config = test_config.from_config
        param = {
            "gmx2_pool": pd.read_feather(
                "samples/arbitrum-GmxV2-0x70d95587d40a2caf56bd97485ab3eec10bee6336-2025-04-07.pool.feather"
            ),
            "gmx2_price": pd.read_feather("samples/arbitrum-GmxV2-2025-04-07.price.feather"),
        }
        tick_df = node.prepare_tick_df(param["gmx2_pool"])

        on_chain_value = self.get_on_chain_value()

        tick_df = tick_df[~tick_df.index.duplicated(keep="last")]

        tick_df["indexPrice"] = on_chain_value["WBTC"].apply(lambda x: float(x))
        tick_df[["longPrice", "shortPrice"]] = on_chain_value[["WBTC", "USDC"]].map(lambda x: float(x))
        tick_df = tick_df[tick_df["longPrice"].notnull()]
        # tick_df[["poolValue", "longPnl", "shortPnl", "netPnl"]] = tick_df.apply(calcPoolValue, axis=1)

        pool_net_value_error = (
            (
                (tick_df["poolValue"].apply(lambda x: Decimal(x)) - on_chain_value["poolValue"])
                / on_chain_value["poolValue"]
            )
            .dropna()
            .apply(lambda x: float(x))
            .abs()
        )
        print(pool_net_value_error)
        print(
            pool_net_value_error.mean(),
            pool_net_value_error.max(),
            pool_net_value_error.min(),
            pool_net_value_error.std(),
        )

        pass

    def get_on_chain_value(self):
        on_chain_value = pd.read_csv("./samples/PoolNetValue-0x47c031236e19d024b42f8AE6780E44A573170703-2025-04-07.csv")
        on_chain_value = on_chain_value.rename(
            {
                "0xaf88d065e77c8cc2239327c5edb3a432268e5831": "USDC",
                "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f": "WBTC",
            },
            axis=1,
        )
        on_chain_value["timestamp"] = on_chain_value["timestamp"].apply(lambda x: pd.Timestamp(x, unit="s", tz="UTC"))
        on_chain_value = on_chain_value.set_index("timestamp")
        on_chain_value["USDC"] = on_chain_value["USDC"].apply(
            lambda x: Decimal(ast.literal_eval(x)["min"]) / Decimal(10 ** (30 - 6))
        )
        on_chain_value["WBTC"] = on_chain_value["WBTC"].apply(
            lambda x: Decimal(ast.literal_eval(x)["min"]) / Decimal(10 ** (30 - 8))
        )
        on_chain_value["market_tokens_price"] = on_chain_value["market_tokens_price"].apply(
            lambda x: Decimal(x) / Decimal(10**30)
        )
        on_chain_value["poolValue"] = on_chain_value["poolValue"].apply(lambda x: Decimal(x) / Decimal(10**30))
        on_chain_value["longPnl"] = on_chain_value["longPnl"].apply(lambda x: Decimal(x) / Decimal(10**30))
        on_chain_value["shortPnl"] = on_chain_value["shortPnl"].apply(lambda x: Decimal(x) / Decimal(10**30))
        on_chain_value["netPnl"] = on_chain_value["netPnl"].apply(lambda x: Decimal(x) / Decimal(10**30))
        on_chain_value["longTokenAmount"] = on_chain_value["longTokenAmount"].apply(
            lambda x: Decimal(x) / Decimal(10**8)
        )
        on_chain_value["shortTokenAmount"] = on_chain_value["shortTokenAmount"].apply(
            lambda x: Decimal(x) / Decimal(10**6)
        )
        on_chain_value["longTokenUsd"] = on_chain_value["longTokenUsd"].apply(lambda x: Decimal(x) / Decimal(10**30))
        on_chain_value["shortTokenUsd"] = on_chain_value["shortTokenUsd"].apply(lambda x: Decimal(x) / Decimal(10**30))
        on_chain_value["totalBorrowingFees"] = on_chain_value["totalBorrowingFees"].apply(
            lambda x: Decimal(x) / Decimal(10**30)
        )
        on_chain_value["impactPoolAmount"] = on_chain_value["impactPoolAmount"].apply(
            lambda x: Decimal(x) / Decimal(10**8)
        )
        on_chain_value["borrowingFeePoolFactor"] = on_chain_value["borrowingFeePoolFactor"].apply(
            lambda x: Decimal(x) / Decimal(10**30)
        )
        on_chain_value = on_chain_value[~on_chain_value.index.duplicated(keep="last")]
        return on_chain_value
