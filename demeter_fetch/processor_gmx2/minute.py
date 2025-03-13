import datetime
from datetime import date
from typing import Dict

import pandas as pd
from eth_abi import decode
from tqdm import tqdm

from demeter_fetch import NodeNames
from demeter_fetch.common import DailyNode, DailyParam, get_depend_name
from .gmx2_utils import data_type, data_decoder


class GmxV2Tick(DailyNode):
    name = NodeNames.gmx2_minute

    def _get_file_name(self, param: DailyParam) -> str:
        return (
            f"{self.from_config.chain.name}-GmxV2-{self.from_config.gmx_v2_config.GM_address}-{param.day.strftime('%Y-%m-%d')}.minute"
            + self._get_file_ext()
        )
