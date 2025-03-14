import datetime
from typing import Dict, List

import pandas as pd

from demeter_fetch import NodeNames
from demeter_fetch.common import DailyNode, DailyParam, get_depend_name
from .pool_value_util import calcPoolValue

minute_file_columns = [
    "timestamp",
    "longAmount",
    "shortAmount",
    "virtualSwapInventoryLong",
    "virtualSwapInventoryShort",
    "poolValue",
    "marketTokensSupply",
    "longPrice",
    "shortPrice",
    "indexPrice",
]


class GmxV2Minute(DailyNode):
    name = NodeNames.gmx2_minute

    def _get_file_name(self, param: DailyParam) -> str:
        return (
            f"{self.from_config.chain.name}-GmxV2-{self.from_config.gmx_v2_config.GM_address}-{param.day.strftime('%Y-%m-%d')}.minute"
            + self._get_file_ext()
        )

    @property
    def _parse_date_column(self) -> List[str]:
        return ["timestamp"]

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: datetime.date) -> pd.DataFrame:
        pool_config = self.from_config.gmx_v2_config
        tick_df = data[get_depend_name(NodeNames.gmx2_pool, self.id)]
        price_df = data[get_depend_name(NodeNames.gmx2_price, self.id)]
        price_df = price_df.set_index("timestamp")
        columns_to_bfill = [
            "longAmount",
            "shortAmount",
            "virtualSwapInventoryLong",
            "virtualSwapInventoryShort",
            "marketTokensSupply",
            "impactPoolAmount",
            "totalBorrowingFees",
            "openInterestLongIsLong",
            "openInterestLongNotLong",
            "openInterestShortIsLong",
            "openInterestShortNotLong",
            "openInterestInTokensLongIsLong",
            "openInterestInTokensLongNotLong",
            "openInterestInTokensShortIsLong",
            "openInterestInTokensShortNotLong",
        ]
        tick_df[columns_to_bfill] = tick_df[columns_to_bfill].bfill()
        tick_df = tick_df.set_index("timestamp")
        minute_df = tick_df.resample("1min").first()
        minute_df[columns_to_bfill] = minute_df[columns_to_bfill].bfill()
        useful_price = pd.DataFrame(
            {
                "longPrice": price_df[pool_config.long_token.name.upper()],
                "shortPrice": price_df[pool_config.short_token.name.upper()],
                "indexPrice": price_df[pool_config.index_token.name.upper()],
            }
        )
        minute_df.index = minute_df.index.tz_localize(None)

        minute_df = pd.concat([minute_df, useful_price], axis=1)
        mask = pd.isna(minute_df["poolValue"])
        minute_df.loc[mask, ["poolValue", "longPnl", "shortPnl", "netPnl"]] = minute_df.loc[mask].apply(
            calcPoolValue, axis=1
        )
        minute_df["timestamp"] = minute_df.index
        minute_df = minute_df[minute_file_columns]
        return minute_df
