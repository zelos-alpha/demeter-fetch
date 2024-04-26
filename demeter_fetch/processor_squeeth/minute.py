from typing import List

from demeter_fetch import NodeNames, Config, DappType, UniswapConfig, TokenConfig
from demeter_fetch.common import DailyNode, DailyParam
import copy


class SqueethMinute(DailyNode):
    name = NodeNames.osqth_minute

    def _get_file_name(self, param: DailyParam) -> str:
        return (
            f"{self.from_config.chain.name}-squeeth-controller-{param.day.strftime('%Y-%m-%d')}.minute"
            + self._get_file_ext()
        )

    def get_config_for_depend(self, depend_name: str) -> List[Config]:
        match depend_name:
            case "uni_rel_price":
                osqth_eth_config: Config = copy.deepcopy(self.config)
                osqth_eth_config.from_config.dapp_type = DappType.uniswap
                osqth_eth_config.from_config.uniswap_config = UniswapConfig(
                    pool_address="0x82c427adfdf2d245ec51d8046b41c4ee87f0d29c",
                    token0=TokenConfig("weth", 18),
                    token1=TokenConfig("osqth", 18),
                    is_token0_base=True,
                )

                eth_usdc_config: Config = copy.deepcopy(self.config)
                eth_usdc_config.from_config.dapp_type = DappType.uniswap
                eth_usdc_config.from_config.uniswap_config = UniswapConfig(
                    pool_address="0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
                    token0=TokenConfig("usdc", 6),
                    token1=TokenConfig("weth", 18),
                    is_token0_base=True,
                )
                return [osqth_eth_config, eth_usdc_config]
            case _:
                return [self.config]
