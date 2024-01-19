from datetime import date

import pandas as pd

from .chifra_utils import query_log_by_chifra
from .source_utils import ContractConfig
from .. import ChainTypeConfig
from ..common import FromConfig, Config, constants


def chifra_pool(config: FromConfig, save_path: str, day: date) -> pd.DataFrame:
    df = query_log_by_chifra(
        chain=config.chain,
        day=day,
        contract=ContractConfig(
            config.uniswap_config.pool_address,
            [constants.SWAP_KECCAK, constants.BURN_KECCAK, constants.COLLECT_KECCAK, constants.MINT_KECCAK],
        ),
        to_path=save_path,
        keep_tmp_files=False,
        http_proxy=config.http_proxy,
        etherscan_api_key=config.chifra_config.etherscan_api_key,
    )
    return df


def chifra_proxy_lp(config: FromConfig, save_path: str, day: date) -> pd.DataFrame:
    df = query_log_by_chifra(
        chain=config.chain,
        day=day,
        contract=ContractConfig(
            ChainTypeConfig[config.chain]["uniswap_proxy_addr"],
            [constants.INCREASE_LIQUIDITY, constants.DECREASE_LIQUIDITY, constants.COLLECT],
        ),
        to_path=save_path,
        keep_tmp_files=False,
        http_proxy=config.http_proxy,
        etherscan_api_key=config.chifra_config.etherscan_api_key,
    )
    return df


def chifra_proxy_transfer(config: FromConfig, save_path: str, day: date) -> pd.DataFrame:
    df = query_log_by_chifra(
        chain=config.chain,
        day=day,
        contract=ContractConfig(
            ChainTypeConfig[config.chain]["uniswap_proxy_addr"],
            [constants.TRANSFER_KECCAK],
        ),
        to_path=save_path,
        keep_tmp_files=False,
        http_proxy=config.http_proxy,
        etherscan_api_key=config.chifra_config.etherscan_api_key,
    )
    return df
