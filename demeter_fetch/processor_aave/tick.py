from datetime import date
from typing import Dict, List

import pandas as pd
import demeter_fetch.processor_aave.aave_utils as aave_utils
from demeter_fetch import NodeNames
from demeter_fetch.common import AaveDailyNode, get_tx_type, KECCAK, get_depend_name
from demeter_fetch.common.nodes import AaveDailyParam


class AaveTick(AaveDailyNode):
    name = NodeNames.aave_tick

    def _get_file_name(self, param: AaveDailyParam) -> str:
        return (
            f"{self.from_config.chain.name}-aave_v3-{param.token}-{param.day.strftime('%Y-%m-%d')}.tick"
            + self._get_file_ext()
        )

    def _process_one_day(self, data: Dict[str, Dict[str, pd.DataFrame]], day: date, tokens) -> Dict[str, pd.DataFrame]:
        ret: Dict[str, pd.DataFrame] = {}
        df = data[get_depend_name(NodeNames.aave_raw, self.id)]
        for token, token_df in df.items():
            token_df["tx_type"] = token_df.apply(lambda x: get_tx_type(x.topics), axis=1)
            token_df = token_df[
                token_df["tx_type"].isin(
                    [
                        KECCAK.AAVE_REPAY,
                        KECCAK.AAVE_BORROW,
                        KECCAK.AAVE_SUPPLY,
                        KECCAK.AAVE_WITHDRAW,
                        KECCAK.AAVE_LIQUIDATION,
                    ]
                )
            ]
            ret[token] = preprocess_one(token_df)
        return ret

    @property
    def _parse_date_column(self) -> List[str]:
        return ["block_timestamp"]


def preprocess_one(df: pd.DataFrame):
    append_columns = ["reserve", "owner", "amount", "liquidator", "debt_asset", "debt_amount", "atoken"]
    ret_df = pd.DataFrame()
    if df.empty:
        for column in df.columns:
            ret_df[column] = None
        return ret_df
    ret_df[append_columns] = df.apply(
        lambda x: aave_utils.handle_event(x.tx_type, x.topics, x.data),
        axis=1,
        result_type="expand",
    )
    ret_df[["block_number", "transaction_hash", "block_timestamp", "transaction_index", "log_index", "tx_type"]] = df[
        ["block_number", "transaction_hash", "block_timestamp", "transaction_index", "log_index", "tx_type"]
    ]
    ret_df["tx_type"] = ret_df.apply(lambda x: x.tx_type.name, axis=1)
    final_order = [
        "block_number",
        "block_timestamp",
        "tx_type",
        "transaction_hash",
        "transaction_index",
        "log_index",
        "reserve",
        "owner",
        "amount",
        "liquidator",
        "debt_asset",
        "debt_amount",
        "atoken",
    ]
    ret_df = ret_df[final_order]

    return ret_df
