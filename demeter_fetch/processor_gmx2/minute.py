import datetime
from typing import Dict, List

import pandas as pd

from demeter_fetch import NodeNames
from demeter_fetch.common import DailyNode, DailyParam, get_depend_name
from .pool_value_util import calcPoolValue

minute_file_columns = [
    "timestamp",
    "marketTokensSupply",
    "poolValue",
    "longAmount",
    "shortAmount",
    "pendingPnl",
    "realizedNetYield",
    "virtualSwapInventoryLong",
    "virtualSwapInventoryShort",
    "impactPoolAmount",
    "longPrice",
    "shortPrice",
    "indexPrice",
]


columns_to_bfill = [
    "longAmount",
    "shortAmount",
    "virtualSwapInventoryLong",
    "virtualSwapInventoryShort",
    "marketTokensSupply",
    "impactPoolAmount",
    "openInterestLongIsLong",
    "openInterestLongNotLong",
    "openInterestShortIsLong",
    "openInterestShortNotLong",
    "openInterestInTokensLongIsLong",
    "openInterestInTokensLongNotLong",
    "openInterestInTokensShortIsLong",
    "openInterestInTokensShortNotLong",
]


def get_net_yield(tick_df: pd.DataFrame) -> pd.Series:
    return pd.Series()


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

    def prepare_tick_df(self, tick_df: pd.DataFrame) -> pd.DataFrame:

        tick_df[columns_to_bfill] = tick_df[columns_to_bfill].bfill()
        tick_df["borrowingFeePoolFactor"] = tick_df["borrowingFeePoolFactor"].ffill().bfill()
        cum_sum_borrowingFeeUsd = 0
        # fill empty totalBorrowingFees,fill all empty then fill first empty rows
        tick_df["totalBorrowingFees"] = tick_df["totalBorrowingFees"].ffill().bfill()
        last_value = -1
        for idx, row in tick_df.iterrows():
            # pending_borrowing_fee = total_borrowing_fee - totalBorrowingFees
            #                       = currumlate_borrowing_factor * all_history_open_interest - totalBorrowingFees
            # when deposit and withdraw, we can get accurate totalBorrowingFees. we will keep this value.
            # and when decrease position, position borrowing fee will be transferred from pending to realized totalBorrowingFees.
            # so we add position borrowing fee to totalBorrowingFees to avoid double count.
            # on next deposit and withdraw, clear the borrowingFeeUsd sum, and use the new accurate totalBorrowingFees value
            if last_value == -1 or row["totalBorrowingFees"] != last_value:  # when totalBorrowingFees changed
                cum_sum_borrowingFeeUsd = 0
                last_value = row["totalBorrowingFees"]
            cum_sum_borrowingFeeUsd += row["borrowingFeeUsd"]  #
            tick_df.loc[idx, "totalBorrowingFees"] -= cum_sum_borrowingFeeUsd
        tick_df["totalBorrowingFees"] = tick_df["totalBorrowingFees"].shift().bfill()
        tick_df = tick_df.set_index("timestamp")
        return tick_df

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: datetime.date) -> pd.DataFrame:
        """
        误差分析:
        * amount: 能够获取当前时刻的精确值.
        * 各个token价格: 由于价格通过插值得到, 所以有误差. 但这个误差不会太大, 因为每个交易都能获取到价格而且交易频率足够高.
        * pending pnl: 取决于openInterest和index price, 其中openInterest是准确的.
        * pending borrowing fee: 取决于total borrowing fee(无法从log中获得), openInterest, pending currumlate borrowing factor(依赖于时间), 所以不容易准确计算. 且pending borrowing fee是一个差值, 因此造成影响较小
        * pending position price impact: 可以准确计算, indexPrice会有一些影响但不大, 至于和virtual price impact比较(更新不及时)的部分, 二者差值不会太大. 所以会有误差但可以接受.

        :param data:
        :param day:
        :return:
        """
        pool_config = self.from_config.gmx_v2_config
        tick_df = data[get_depend_name(NodeNames.gmx2_pool, self.id)]
        price_df = data[get_depend_name(NodeNames.gmx2_price, self.id)]
        price_df = price_df.set_index("timestamp")

        tick_df = self.prepare_tick_df(tick_df)
        minute_df = tick_df.resample("1min").first()
        minute_df["longAmountYield"] = (
            (tick_df["longAmountDelta"] - tick_df["longAmountDeltaNoFee"]).resample("1min").sum()
        )
        minute_df["shortAmountYield"] = (
            (tick_df["shortAmountDelta"] - tick_df["shortAmountDeltaNoFee"]).resample("1min").sum()
        )
        minute_df.index = minute_df.index.tz_localize(None)

        new_index = pd.date_range(
            start=pd.Timestamp(day),
            end=pd.Timestamp(day) + pd.Timedelta(days=1) - pd.Timedelta(minutes=1),
            freq="min",
        )
        minute_df = minute_df.reindex(new_index).infer_objects(copy=False)

        minute_df[columns_to_bfill] = minute_df[columns_to_bfill].bfill()
        minute_df[["totalBorrowingFees", "borrowingFeePoolFactor"]] = minute_df[
            ["totalBorrowingFees", "borrowingFeePoolFactor"]
        ].bfill()
        minute_df[["longAmountYield", "shortAmountYield"]] = minute_df[["longAmountYield", "shortAmountYield"]].fillna(
            0
        )
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
        # Reframe the PnL from the LP’s perspective, as the LP acts as the counterparty to the Open Interest.
        minute_df["netPnl"] = -minute_df["netPnl"]
        minute_df.rename(columns={"netPnl": "pendingPnl"}, inplace=True)

        minute_df["realizedNetYield"] = (
            minute_df["longAmountYield"] * minute_df["longPrice"]
            + minute_df["shortAmountYield"] * minute_df["shortPrice"]
        )

        minute_df = minute_df[minute_file_columns]

        return minute_df
