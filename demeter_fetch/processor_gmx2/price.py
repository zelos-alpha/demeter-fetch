from datetime import date, datetime, timezone
from typing import Dict, List

import pandas as pd
from eth_abi import decode
from tqdm import tqdm

from demeter_fetch import NodeNames, ChainType
from demeter_fetch.common import DailyNode, DailyParam, get_depend_name
from .gmx2_utils import GmxTopics, data_type, data_decoder, arb_tokens


class GmxV2Price(DailyNode):
    def __init__(self):
        super().__init__()
        self.execute_in_sub_process = True

    name = NodeNames.gmx2_price

    def _get_file_name(self, param: DailyParam) -> str:
        return f"{self.from_config.chain.name}-GmxV2-{param.day.strftime('%Y-%m-%d')}.price" + self._get_file_ext()

    @property
    def _parse_date_column(self) -> List[str]:
        return ["timestamp"]

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: date) -> pd.DataFrame:
        if self.from_config.chain == ChainType.arbitrum:
            known_tokens = arb_tokens
        else:
            raise RuntimeError("Doesn't have token information for " + self.from_config.chain.name)
        input_df = data[get_depend_name(NodeNames.gmx2_raw, self.id)]
        input_df = input_df[input_df["topics"].str[0] == GmxTopics.EventLog1.value]
        input_df = input_df[input_df["topics"].str[1] == GmxTopics.OraclePriceUpdate.value]
        time_idx = {}
        time_list = []
        with tqdm(total=len(input_df.index), ncols=60, position=1, leave=False) as pbar:
            for i, row in input_df.iterrows():
                decoded_data = decode(data_type, bytes.fromhex(row["data"][2:]))
                values = data_decoder(decoded_data[2])
                address = values["token"].lower()
                if address not in known_tokens.keys():
                    pbar.update()
                    continue
                if values["timestamp"] not in time_idx.keys():
                    time_list.append({})
                    time_idx[values["timestamp"]] = len(time_list) - 1
                time_list[time_idx[values["timestamp"]]][address] = (values["minPrice"] + values["maxPrice"]) // 2
                pbar.update()

        sorted_time = [k for k, v in sorted(time_idx.items(), key=lambda item: item[1])]
        sorted_time = [datetime.fromtimestamp(i, tz=timezone.utc) for i in sorted_time]
        df = pd.DataFrame(time_list, index=sorted_time)
        for i in df.columns:
            df[i] = df[i] * 10 ** (known_tokens[i][1] - 30)

        rename_dict = {k: v[0] for k, v in known_tokens.items()}
        price_df = df.rename(columns=rename_dict)

        new_index = pd.date_range(
            start=pd.Timestamp(day),
            end=pd.Timestamp(day) + pd.Timedelta(days=1) - pd.Timedelta(minutes=1),
            freq="min",
        )
        price_df = price_df[~price_df.index.duplicated(keep="last")]  # remove duplicate index
        price_df = price_df.resample("1min").last()
        # expend to whole day, and fill tail and head empty minutes
        price_df.index = price_df.index.tz_localize(None)
        price_df = price_df.reindex(new_index).infer_objects(copy=False).ffill().bfill()
        price_df.insert(0, "timestamp", price_df.index)
        return price_df
