from demeter_fetch import  SqueethNodesNames
from demeter_fetch.common import DailyNode, DailyParam


class SqueethMinute(DailyNode):
    def __init__(self, depends):
        super().__init__(depends)
        self.name = SqueethNodesNames.minute

    def _get_file_name(self, param: DailyParam) -> str:
        return (
            f"{self.from_config.chain.name}-squeeth-controller-{param.day.strftime('%Y-%m-%d')}.minute"
            + self._get_file_ext()
        )
