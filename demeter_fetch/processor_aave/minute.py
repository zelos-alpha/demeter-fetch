from typing import Dict, List

import pandas as pd

from demeter_fetch import KECCAK, NodeNames
from demeter_fetch.common import AaveDailyNode, get_tx_type, get_depend_name
from demeter_fetch.common.nodes import AaveDailyParam
from demeter_fetch.processor_aave.aave_utils import decode_event_ReserveDataUpdated
from datetime import datetime, date


class AaveMinute(AaveDailyNode):
    name = NodeNames.aave_minute

    def _get_file_name(self, param: AaveDailyParam) -> str:
        return (
            f"{self.from_config.chain.name}-aave_v3-{param.token}-{param.day.strftime('%Y-%m-%d')}.minute"
            + self._get_file_ext()
        )

    def _process_one_day(self, data: Dict[str, Dict[str, pd.DataFrame]], day: date, tokens) -> Dict[str, pd.DataFrame]:
        ret: Dict[str, pd.DataFrame] = {}
        df = data[get_depend_name(NodeNames.aave_raw, self.id)]
        for token, token_df in df.items():
            token_df["tx_type"] = token_df.apply(lambda x: get_tx_type(x.topics), axis=1)
            token_df = token_df[token_df["tx_type"] == KECCAK.AAVE_UPDATED]
            ret[token] = preprocess_one(token_df)
        return ret

    @property
    def _parse_date_column(self) -> List[str]:
        return ["block_timestamp"]


def preprocess_one(raw_df: pd.DataFrame):
    result_df = pd.DataFrame()
    result_df[
        [
            "liquidity_rate",
            "stable_borrow_rate",
            "variable_borrow_rate",
            "liquidity_index",
            "variable_borrow_index",
        ]
    ] = raw_df.apply(decode_event_ReserveDataUpdated, axis=1, result_type="expand")
    result_df["block_timestamp"] = pd.to_datetime(raw_df["block_timestamp"])
    result_df = result_df.set_index("block_timestamp")
    if len(result_df.index) == 0:  # if empty
        return result_df
    # add start and end of the day, so after resample, there will always be 1440 row
    # please add tail first, because new line will always be added to tail.
    day_end = datetime.combine(result_df.index[0].date(), datetime.max.time(), result_df.index[0].tzinfo)
    result_df.loc[day_end] = result_df.tail(1).iloc[0]

    day_start = datetime.combine(result_df.index[0].date(), datetime.min.time(), result_df.index[0].tzinfo)
    if day_start not in result_df.index:
        result_df.loc[day_start] = result_df.head(1).iloc[0]
    result_df = result_df.resample("1min").last().ffill().shift(1).bfill()

    result_df.insert(loc=0, column="block_timestamp", value=result_df.index)

    return result_df
