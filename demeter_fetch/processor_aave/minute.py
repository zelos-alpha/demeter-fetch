import pandas as pd

from demeter_fetch.processor_aave.aave_utils import decode_event_ReserveDataUpdated
from datetime import datetime


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
