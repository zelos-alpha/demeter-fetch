from datetime import date

import pandas as pd

from .chifra_utils import query_log_by_chifra
from .source_utils import ContractConfig
from .. import ChainTypeConfig
from ..common import FromConfig
import demeter_fetch.common._typing as TYPE


def chifra_pool(config: FromConfig, save_path: str, day: date) -> pd.DataFrame:
    df = query_log_by_chifra(
        chain=config.chain,
        day=day,
        contract=ContractConfig(
            config.uniswap_config.pool_address,
            [TYPE.KECCAK.SWAP.value, TYPE.KECCAK.BURN.value, TYPE.KECCAK.COLLECT.value, TYPE.KECCAK.MINT.value],
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
            [
                TYPE.KECCAK.UNI_PROXY_DECREASE.value,
                TYPE.KECCAK.UNI_PROXY_INCREASE.value,
                TYPE.KECCAK.UNI_PROXY_COLLECT.value,
            ],
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
            [TYPE.KECCAK.TRANSFER.value],
        ),
        to_path=save_path,
        keep_tmp_files=False,
        http_proxy=config.http_proxy,
        etherscan_api_key=config.chifra_config.etherscan_api_key,
    )
    return df


def chifra_aave(config: FromConfig, save_path: str, day: date, tokens) -> pd.DataFrame:
    df = query_log_by_chifra(
        chain=config.chain,
        day=day,
        contract=ContractConfig(
            ChainTypeConfig[config.chain]["aave_v3_pool_addr"],
            [
                TYPE.KECCAK.AAVE_REPAY.value,
                TYPE.KECCAK.AAVE_BORROW.value,
                TYPE.KECCAK.AAVE_SUPPLY.value,
                TYPE.KECCAK.AAVE_WITHDRAW.value,
                TYPE.KECCAK.AAVE_UPDATED.value,
                TYPE.KECCAK.AAVE_LIQUIDATION.value,
            ],
        ),
        to_path=save_path,
        keep_tmp_files=False,
        http_proxy=config.http_proxy,
        etherscan_api_key=config.chifra_config.etherscan_api_key,
    )
    return df
