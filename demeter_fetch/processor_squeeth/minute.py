from datetime import date
from decimal import Decimal
from typing import List, Dict

import numpy as np
import pandas as pd

from demeter_fetch import NodeNames, Config, DappType, UniswapConfig, TokenConfig
from demeter_fetch.common import DailyNode, DailyParam, get_depend_name
import copy


class SqueethMinute(DailyNode):
    name = NodeNames.osqth_minute

    def _get_file_name(self, param: DailyParam) -> str:
        return (
            f"{self.from_config.chain.name}-squeeth-controller-{param.day.strftime('%Y-%m-%d')}.minute"
            + self._get_file_ext()
        )

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: date) -> pd.DataFrame:
        squeeth_price_df = data[get_depend_name(NodeNames.uni_relative_price, "osqth-price")]
        squeeth_price_df = squeeth_price_df.set_index(["block_timestamp"])
        eth_price_df = data[get_depend_name(NodeNames.uni_relative_price, "eth-price")]
        eth_price_df = eth_price_df.set_index(["block_timestamp"])

        raw_df = data[get_depend_name(NodeNames.osqth_raw, self.id)]
        if len(raw_df.index) < 1:
            # in case this day is empty
            new_index = pd.date_range(
                start=pd.to_datetime(day).floor("D"),
                end=pd.to_datetime(day) + pd.Timedelta(days=1) - pd.Timedelta(minutes=1),
                freq="min",
            )
            df = pd.DataFrame(
                index=new_index,
                columns=["norm_factor", "WETH", "OSQTH"],
                data=np.nan,
            )
            df["WETH"] = eth_price_df["weth"]
            df["OSQTH"] = squeeth_price_df["osqth"]
            return df
        raw_df["block_timestamp"] = pd.to_datetime(raw_df["block_timestamp"].apply(lambda x: x[0:19]))
        raw_df = raw_df.set_index(["block_timestamp"])
        raw_df["oldNormFactor"] = raw_df["data"].apply(lambda x: Decimal(int(x[2 : 2 + 64], 16)) / Decimal(1e18))
        raw_df["newNormFactor"] = raw_df["data"].apply(
            lambda x: Decimal(int(x[2 + 64 : 2 + 64 * 2], 16)) / Decimal(1e18)
        )
        first_old_norm_factor = raw_df.iloc[0]["oldNormFactor"]
        new_index = pd.date_range(
            start=raw_df.index[0].floor("D"),
            end=raw_df.index[0].floor("D") + pd.Timedelta(days=1) - pd.Timedelta(minutes=1),
            freq="min",
        )
        raw_df.drop(
            columns=[
                "oldNormFactor",
                "data",
                "topics",
                "block_number",
                "transaction_hash",
                "transaction_index",
                "log_index",
            ],
            inplace=True,
        )
        raw_df = raw_df[~raw_df.index.duplicated(keep="last")]
        # resample to 1 min
        # 1. in every minute, use the last value of the minute
        # 2. fill empty minute with previous minute
        # 3. shift one minute, this will ensure value in 0 second is the right value
        #    (no transactions so it will be the same to the last minute, e.g. if tx at 8:00:15 make price to 1000,
        #    at 8:01:00 price should be 1000, regardless any transaction happens during 8:01)
        # 4. fill the first minute with the value of second minute
        raw_df = raw_df.resample("1min").last().ffill().shift(1).bfill()
        raw_df = raw_df.reindex(new_index).ffill()
        raw_df["newNormFactor"] = raw_df["newNormFactor"].fillna(value=first_old_norm_factor)
        raw_df["WETH"] = eth_price_df["weth"]
        raw_df["OSQTH"] = squeeth_price_df["osqth"]
        raw_df = raw_df.rename(columns={"newNormFactor": "norm_factor"})
        raw_df["block_timestamp"] = raw_df.index
        raw_df = raw_df[["block_timestamp", "norm_factor", "WETH", "OSQTH"]]
        return raw_df

    def get_config_for_depend(self, depend_name: str) -> List[Config]:
        match depend_name:
            case "uni_rel_price":
                osqth_eth_config: Config = copy.deepcopy(self.config)
                osqth_eth_config.id = "osqth-price"
                osqth_eth_config.from_config.dapp_type = DappType.uniswap
                osqth_eth_config.from_config.uniswap_config = UniswapConfig(
                    pool_address="0x82c427adfdf2d245ec51d8046b41c4ee87f0d29c",
                    token0=TokenConfig("weth", 18),
                    token1=TokenConfig("osqth", 18),
                    is_token0_base=True,
                )

                eth_usdc_config: Config = copy.deepcopy(self.config)
                eth_usdc_config.id = "eth-price"
                eth_usdc_config.from_config.dapp_type = DappType.uniswap
                eth_usdc_config.from_config.uniswap_config = UniswapConfig(
                    pool_address="0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
                    token0=TokenConfig("usdc", 6),
                    token1=TokenConfig("weth", 18),
                    is_token0_base=True,
                )
                return [osqth_eth_config, eth_usdc_config]
            case _:
                return [self.config]
