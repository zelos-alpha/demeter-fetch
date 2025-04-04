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
    "impactPoolAmount",
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
        minute_df.index = minute_df.index.tz_localize(None)

        new_index = pd.date_range(
            start=pd.Timestamp(day),
            end=pd.Timestamp(day) + pd.Timedelta(days=1) - pd.Timedelta(minutes=1),
            freq="min",
        )
        minute_df = minute_df.reindex(new_index).infer_objects(copy=False)

        minute_df[columns_to_bfill] = minute_df[columns_to_bfill].bfill()
        useful_price = pd.DataFrame(
            {
                "longPrice": price_df[pool_config.long_token.name.upper()],
                "shortPrice": price_df[pool_config.short_token.name.upper()],
                "indexPrice": price_df[pool_config.index_token.name.upper()],
            }
        )
        minute_df.index = minute_df.index.tz_localize(None)
        useful_price.index = useful_price.index.tz_localize(None)

        minute_df = pd.concat([minute_df, useful_price], axis=1)

        """
        在这里, 我们覆盖掉从deposit和withdraw中获取的pool value和pnl, 
        这样做的原因是, log中获取的value和计算的value存在误差. 
        已知calcPoolValue的算法没问题, 如果使用MarketPoolValueInfo中获取的值计算的话, 误差在10^-7. 
        所以这里的误差, 是由于数据的时间没有对齐引起的. 这是无法避免的, 为了避免数据波动这里poolvalue统一使用计算值
        
        Here, we override the pool value and PnL obtained from `deposit` and `withdraw`. 
        The reason for this is that there is a discrepancy between the value obtained from the log and the calculated value. 
        It is known that the algorithm for `calcPoolValue` is correct. 
        If we use the values obtained from `MarketPoolValueInfo` for calculation, 
        the error is on the order of 10⁻⁷.
         Therefore, the error here is caused by the misalignment of data timestamps, which is unavoidable. 
         To prevent data fluctuations, the pool value is uniformly set to the calculated value in this case.
        """
        # mask = pd.isna(minute_df["poolValue"])
        # minute_df.loc[mask, ["poolValue", "longPnl", "shortPnl", "netPnl"]] = minute_df.loc[mask].apply(
        #     calcPoolValue, axis=1
        # )

        minute_df[["poolValue", "longPnl", "shortPnl", "netPnl"]] = minute_df.apply(calcPoolValue, axis=1)
        minute_df["timestamp"] = minute_df.index
        minute_df = minute_df[minute_file_columns]
        return minute_df
