import datetime
from datetime import date
from typing import Dict, List

import pandas as pd
from eth_abi import decode
from tqdm import tqdm

from demeter_fetch import NodeNames
from demeter_fetch.common import DailyNode, DailyParam, get_depend_name
from .gmx2_utils import data_type, data_decoder

tick_file_columns = [
    "block_number",
    "block_timestamp",
    "transaction_hash",
    "tx_index",
    "log_index",
    "tx_type",
    "sender",
    "event_name_hash",
    "event_name",
    "market",
    "topic1",
    "topic2",
    "data",
]


class GmxV2Tick(DailyNode):
    def __init__(self):
        super().__init__()
        self.execute_in_sub_process = True

    name = NodeNames.gmx2_tick

    def _get_file_name(self, param: DailyParam) -> str:
        return f"{self.from_config.chain.name}-GmxV2-{param.day.strftime('%Y-%m-%d')}.tick" + self._get_file_ext()

    @property
    def _parse_date_column(self) -> List[str]:
        return ["block_timestamp"]

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: date) -> pd.DataFrame:
        input_df = data[get_depend_name(NodeNames.gmx2_raw, self.id)]  # .head(2000)
        result_list = []
        block_time = {}
        with tqdm(total=len(input_df.index), ncols=60, position=1, leave=False) as pbar:
            for index, row in input_df.iterrows():
                decoded_data = decode(data_type, bytes.fromhex(row["data"][2:]))

                log_item = {
                    "block_number": row.block_number,
                    "transaction_hash": row.transaction_hash,
                    "tx_index": row.transaction_index,
                    "log_index": row.log_index,
                    "event_name_hash": row["topics"][1],
                    "sender": decoded_data[0],
                    "event_name": decoded_data[1],
                    "data": data_decoder(decoded_data[2]),
                    "block_timestamp": datetime.datetime.min,  # set a initial value to avoid memory relocation
                }
                if len(row["topics"]) > 2:
                    log_item["topic1"] = row["topics"][2]
                if len(row["topics"]) > 3:
                    log_item["topic2"] = row["topics"][3]
                if "market" in log_item["data"]:
                    log_item["market"] = log_item["data"]["market"]
                    del log_item["data"]["market"]
                result_list.append(log_item)
                if log_item["event_name"] == "OraclePriceUpdate":
                    block_time[row.block_number] = datetime.datetime.fromtimestamp(
                        log_item["data"]["timestamp"], datetime.timezone.utc
                    )
                elif "updatedAtTime" in log_item["data"]:
                    block_time[row.block_number] = datetime.datetime.fromtimestamp(
                        log_item["data"]["updatedAtTime"], datetime.timezone.utc
                    )
                pbar.update()
        for item in result_list:
            if item["block_number"] in block_time:
                item["block_timestamp"] = block_time[item["block_number"]]
        df = pd.DataFrame(result_list)
        df = df.sort_values(by=["block_number", "tx_index", "log_index"])
        df["tx_type"] = df.groupby("transaction_hash")["event_name"].transform(
            lambda x: ";".join(x[x.str.contains("Executed")]).replace("Executed", "")
        )
        df = df[tick_file_columns]
        df["data"] = df["data"].astype(str)
        return df
