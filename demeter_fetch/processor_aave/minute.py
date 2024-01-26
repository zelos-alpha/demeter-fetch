from typing import Dict

import pandas as pd

from demeter_fetch import AaveNodesNames,KECCAK
from demeter_fetch.common import AaveDailyNode, get_tx_type
from demeter_fetch.common.nodes import AaveDailyParam
from demeter_fetch.processor_aave.aave_utils import decode_event_ReserveDataUpdated
from datetime import datetime, date


class AaveMinute(AaveDailyNode):
    def __init__(self, depends):
        super().__init__(depends)
        self.name = AaveNodesNames.minute

    def _get_file_name(self, param: AaveDailyParam) -> str:
        return f"{self.from_config.chain.name}-aave_v3-{param.token}-{param.day.strftime('%Y-%m-%d')}.minute.csv"

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: date) -> Dict[str, pd.DataFrame]:
        ret = {}
        for token, raw_df in data.items():
            df["tx_type"] = df.apply(lambda x: get_tx_type(x.topics), axis=1)
            df = df[df["tx_type"] == KECCAK.AAVE_UPDATED]
            ret[token] = preprocess_one(raw_df)
        return ret


def preprocess_one(raw_df: pd.DataFrame):
    raw_df[
        [
            "liquidity_rate",
            "stable_borrow_rate",
            "variable_borrow_rate",
            "liquidity_index",
            "variable_borrow_index",
        ]
    ] = raw_df.apply(decode_event_ReserveDataUpdated, axis=1, result_type="expand")

    raw_df = raw_df.drop(
        columns=[
            "block_number",
            "transaction_hash",
            "transaction_index",
            "log_index",
            "topics",
            "DATA",
            "token",
        ]
    )
    raw_df = raw_df.set_index("block_timestamp")
    if len(raw_df.index) == 0:  # if empty
        return raw_df
    # add start and end of the day, so after resample, there will always be 1440 row
    # please add tail first, because new line will always be added to tail.
    day_end = datetime.combine(raw_df.index[0].date(), datetime.max.time(), raw_df.index[0].tzinfo)
    raw_df.loc[day_end] = raw_df.tail(1).iloc[0]

    day_start = datetime.combine(raw_df.index[0].date(), datetime.min.time(), raw_df.index[0].tzinfo)
    if day_start not in raw_df.index:
        raw_df.loc[day_start] = raw_df.head(1).iloc[0]
    raw_df = raw_df.resample("1T").last().ffill()
    return raw_df
