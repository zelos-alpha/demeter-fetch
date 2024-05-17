from datetime import datetime
from typing import Callable, Dict, List

import pandas as pd

from demeter_fetch import Config
from demeter_fetch.common import DailyNode, DailyParam, get_tx_type, to_decimal, to_int
from demeter_fetch.common import KECCAK, UniNodesNames
from .uniswap_utils import x96_sqrt_to_decimal


class UniRelativePrice(DailyNode):
    """
    Will generate daily file.
    row is minute.
    column is token
    """

    def __init__(self, depends):
        super().__init__(depends)
        self.name = UniNodesNames.relative_price

    def _get_file_name(self, param: DailyParam) -> str:
        return (
            f"{self.from_config.chain.name}-{self.from_config.uniswap_config.pool_address}-{param.day.strftime('%Y-%m-%d')}.rel-price"
            + self._get_file_ext()
        )

    @property
    def _parse_date_column(self) -> List[str]:
        return ["block_timestamp"]

    def set_config(self, config: Config):
        super().set_config(config)
        if config.from_config.uniswap_config.is_token0_base is None:
            raise RuntimeError("should set uniswap_config.is_token0_base")
        if config.from_config.uniswap_config.token0 is None:
            raise RuntimeError("should set uniswap_config.token0")
        if config.from_config.uniswap_config.token1 is None:
            raise RuntimeError("should set uniswap_config.token1")

    @property
    def _load_csv_converter(self) -> Dict[str, Callable]:
        return {
            "token0": to_decimal,
            "token1": to_decimal,
            "total_liquidity": to_int,
            "sqrtPriceX96": to_int,
        }

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: datetime.date):
        df = data[UniNodesNames.tick_without_pos]
        price_df = df[df["tx_type"] == "SWAP"].copy()
        price_df.set_index("block_timestamp", inplace=True)
        price_df["price"] = price_df["sqrtPriceX96"].apply(
            lambda a: x96_sqrt_to_decimal(
                a,
                self.from_config.uniswap_config.token0.decimal,
                self.from_config.uniswap_config.token1.decimal,
                self.from_config.uniswap_config.is_token0_base,
            )
        )

        price_df = price_df[["price", "total_liquidity", "sqrtPriceX96"]]
        if self.from_config.uniswap_config.is_token0_base:
            price_df.rename(columns={"price": "token1"}, inplace=True)
            price_df["token0"] = 1
        else:
            price_df.rename(columns={"price": "token0"}, inplace=True)
            price_df["token1"] = 1

        price_df = price_df.resample("1min").last().bfill()
        price_df["block_timestamp"] = price_df.index
        price_df = price_df[["block_timestamp", "token0", "token1", "total_liquidity", "sqrtPriceX96"]]
        return price_df
