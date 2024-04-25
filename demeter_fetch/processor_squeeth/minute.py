from demeter_fetch import NodeNames
from demeter_fetch.common import DailyNode, DailyParam


class SqueethMinute(DailyNode):
    name = NodeNames.osqth_minute

    def _get_file_name(self, param: DailyParam) -> str:
        return (
            f"{self.from_config.chain.name}-squeeth-controller-{param.day.strftime('%Y-%m-%d')}.minute"
            + self._get_file_ext()
        )
