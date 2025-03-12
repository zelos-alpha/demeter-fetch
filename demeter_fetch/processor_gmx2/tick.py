from datetime import date
from typing import Dict

import pandas as pd

from demeter_fetch import NodeNames
from demeter_fetch.common import DailyNode, DailyParam, get_depend_name


class GmxV2Tick(DailyNode):
    name = NodeNames.gmx2_tick

    def _get_file_name(self, param: DailyParam) -> str:
        return (
            f"{self.from_config.chain.name}-GmxV2-{param.day.strftime('%Y-%m-%d')}.tick"
            + self._get_file_ext()
        )

    def _process_one_day(self, data: Dict[str, pd.DataFrame], day: date) -> pd.DataFrame:
        input_df = data[get_depend_name(NodeNames.gmx2_raw, self.id)]
        pass


