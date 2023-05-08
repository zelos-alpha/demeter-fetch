import unittest
from datetime import datetime
import sys

from demeter_fetch.source_rpc import query_blockno_from_time
import demeter_fetch._typing as typing


class UniLpDataTest(unittest.TestCase):
    # ==========lines=========================
    def test_query_blockno_from_time(self):
        value = query_blockno_from_time(typing.ChainType.ethereum, datetime(2023, 5, 8), True, "127.0.0.1:7890")
        self.assertTrue(value == 17209709, "height not right")
