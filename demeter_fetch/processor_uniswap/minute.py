import datetime

import pandas
import pandas as pd
from pandas import Timestamp

import demeter_fetch.processor_uniswap.uniswap_utils as uniswap_utils
from demeter_fetch._typing import MinuteData, OnchainTxType, MinuteDataNames
from demeter_fetch.utils import TextUtil, TimeUtil, DataUtil


class ModuleUtils(object):
    @staticmethod
    def get_datetime(date_str: str) -> datetime:
        if type(date_str) == Timestamp:
            return date_str.to_pydatetime()
        else:
            return datetime.datetime.strptime(TextUtil.cut_after(str(date_str), "+").replace("T", " "), "%Y-%m-%d %H:%M:%S")


def preprocess_one(raw_data: pd.DataFrame) -> pd.DataFrame:
    if raw_data.size <= 0:
        return raw_data
    start_time = TimeUtil.get_minute(ModuleUtils.get_datetime(raw_data.loc[0, "block_timestamp"]))
    minute_rows = []
    data = []
    total_index = 1
    raw_data["tx_type"] = raw_data.apply(lambda x: uniswap_utils.get_tx_type(x.pool_topics), axis=1)

    for index, row in raw_data.iterrows():
        current_time = TimeUtil.get_minute(ModuleUtils.get_datetime(row["block_timestamp"]))
        if start_time == current_time:  # middle of a minute
            minute_rows.append(row)
        else:  #
            data.append(sample_data_to_one_minute(start_time, minute_rows))
            total_index += 1
            # start on_bar minute
            start_time = current_time
            minute_rows = [row]
    data = DataUtil.fill_missing(data)
    df = pandas.DataFrame(columns=MinuteDataNames, data=map(lambda d: d.to_array(), data))
    return df


def sample_data_to_one_minute(current_time, minute_rows) -> MinuteData:
    data = MinuteData()
    data.timestamp = current_time

    i = 1
    for r in minute_rows:
        (
            sender,
            receipt,
            amount0,
            amount1,
            sqrtPriceX96,
            current_liquidity,
            current_tick,
            tick_lower,
            tick_upper,
            liquidity,
            delta_liquidity,
        ) = uniswap_utils.handle_event(r.tx_type, r.pool_topics, r.pool_data)
        # print(tx_type, sender, receipt, amount0, amount1, sqrtPriceX96, current_liquidity, current_tick, tick_lower,
        #       tick_upper, delta_liquidity)
        match r.tx_type:
            case OnchainTxType.MINT:
                pass
            case OnchainTxType.BURN:
                pass
            case OnchainTxType.COLLECT:
                pass
            case OnchainTxType.SWAP:
                data.netAmount0 += amount0
                data.netAmount1 += amount1
                if amount0 > 0:
                    data.inAmount0 += amount0
                if amount1 > 0:
                    data.inAmount1 += amount1
                if data.openTick is None:  # first
                    data.openTick = current_tick
                    data.highestTick = current_tick
                    data.lowestTick = current_tick
                if data.highestTick < current_tick:
                    data.highestTick = current_tick
                if data.lowestTick > current_tick:
                    data.lowestTick = current_tick
                if i == len(minute_rows):  # last
                    data.closeTick = current_tick
                    data.currentLiquidity = current_liquidity

        i += 1
    return data
