import json
import sys
from typing import NamedTuple, Callable

from tqdm import tqdm
from web3 import Web3

from demeter_fetch.tools.abi import Abis
from demeter_fetch.tools.gmx.keys import (
    deposit_fee_factor_key,
    swap_impact_factor_key,
    swap_impact_exponent_factor_key,
    withdrawal_fee_factor_key,
    swap_fee_factor_key,
    position_impact_exponent_factor_key,
    position_impact_factor_key,
    max_position_impact_factor_key,
    position_fee_factor_key,
    liquidation_fee_factor_key,
    max_pnl_factor_key,
    MAX_PNL_FACTOR_FOR_TRADERS,
    min_collateral_factor_for_open_interest_multiplier_key,
    min_collateral_factor_key,
    MIN_COLLATERAL_USD,
    MIN_POSITION_SIZE_USD,
    SKIP_BORROWING_FEE_FOR_SMALLER_SIDE,
    open_interest_reserve_factor_key,
    base_borrowing_factor_key,
    funding_increase_factor_per_second_key,
    funding_decrease_factor_per_second_key,
    funding_exponent_factor_key,
    funding_factor_key,
    threshold_for_stable_funding_key,
    threshold_for_decrease_funding_key,
    min_funding_factor_per_second_key,
    max_funding_factor_per_second_key,
    max_position_impact_factor_for_liquidations_key,
    min_collateral_factor_for_liquidation_key,
)


class Cfg(NamedTuple):
    end_point: str
    call_back: Callable = None
    height: str | int = None
    proxy: str = None


def get_gmx_pool_config(args):
    cfg = Cfg(
        end_point=args.rpc,
        height=args.block_number,
        proxy=args.proxy,
    )
    pool = Web3.to_checksum_address(args.market)
    # dataStore.getUint(Keys.depositFeeFactorKey(marketToken, balanceWasImproved))
    pool_configs = {
        "swapImpactExponentFactor": swap_impact_exponent_factor_key(pool),
        "swapImpactFactor_Positive": swap_impact_factor_key(pool, True),
        "swapImpactFactor_Negative": swap_impact_factor_key(pool, False),
        "depositFeeFactor_Positive": deposit_fee_factor_key(pool, True),
        "depositFeeFactor_Negative": deposit_fee_factor_key(pool, False),
        "withdrawFeeFactor_Positive": withdrawal_fee_factor_key(pool, True),
        "withdrawFeeFactor_Negative": withdrawal_fee_factor_key(pool, False),
        "swapFeeFactor_BalanceWasImproved": swap_fee_factor_key(pool, True),
        "swapFeeFactor_BalanceNotImproved": swap_fee_factor_key(pool, False),
        "positionImpactExponentFactor_Positive": position_impact_exponent_factor_key(pool, True),
        "positionImpactExponentFactor_Negative": position_impact_exponent_factor_key(pool, False),
        "positionImpactFactor_Positive": position_impact_factor_key(pool, True),
        "positionImpactFactor_Negative": position_impact_factor_key(pool, False),
        "maxPositionImpactFactor_Positive": max_position_impact_factor_key(pool, True),
        "maxPositiveImpactFactor_Negative": max_position_impact_factor_key(pool, False),
        "positionFeeFactor_Positive": position_fee_factor_key(pool, True),
        "positionFeeFactor_Negative": position_fee_factor_key(pool, False),
        "liquidationFeeFactor": liquidation_fee_factor_key(pool),
        "maxPnlFactor_ForTrader_Long": max_pnl_factor_key(MAX_PNL_FACTOR_FOR_TRADERS, pool, True),
        "maxPnlFactor_ForTrader_Short": max_pnl_factor_key(MAX_PNL_FACTOR_FOR_TRADERS, pool, False),
        "minCollateralFactorForOpenInterestMultiplier_Long": min_collateral_factor_for_open_interest_multiplier_key(
            pool, True
        ),
        "minCollateralFactorForOpenInterestMultiplier_Short": min_collateral_factor_for_open_interest_multiplier_key(
            pool, False
        ),
        "minCollateralFactor": min_collateral_factor_key(pool),
        "minCollateralUsd": MIN_COLLATERAL_USD,
        "minPositionSizeUsd": MIN_POSITION_SIZE_USD,
        # "openInterestReserveFactor_Long": open_interest_reserve_factor_key(pool, True),
        # "openInterestReserveFactor_Short": open_interest_reserve_factor_key(pool, False),
        # "baseBorrowingFactor_Long": base_borrowing_factor_key(pool, False),
        # "baseBorrowingFactor_Short": base_borrowing_factor_key(pool, False),
        # "fundingIncreaseFactorPerSecond": funding_increase_factor_per_second_key(pool),
        # "fundingDecreaseFactorPerSecond": funding_decrease_factor_per_second_key(pool),
        # "fundingExponentFactor": funding_exponent_factor_key(pool),
        # "fundingFactor": funding_factor_key(pool),
        # "thresholdForStableFunding": threshold_for_stable_funding_key(pool),
        # "thresholdForDecreaseFunding": threshold_for_decrease_funding_key(pool),
        # "minFundingFactorPerSecond": min_funding_factor_per_second_key(pool),
        # "maxFundingFactorPerSecond": max_funding_factor_per_second_key(pool),
        "maxPositionImpactFactorForLiquidation": max_position_impact_factor_for_liquidations_key(pool),
        "minCollateralFactorForLiquidation": min_collateral_factor_for_liquidation_key(pool),
    }

    values = {}

    with tqdm(total=len(pool_configs)) as pbar:
        for name, config_key in pool_configs.items():
            result = query_data_store("getUint", cfg, key=config_key)
            values[name] = result / 10**30
            pbar.update()
            # print(name, values[name])

    # pool_configs_bool = {
    #     "skip_borrowing_fee_for_smaller_side": SKIP_BORROWING_FEE_FOR_SMALLER_SIDE,
    # }
    # with tqdm(total=len(pool_configs_bool)) as pbar:
    #     for name, config_key in pool_configs_bool.items():
    #         result = query_data_store("getBool", cfg, key=config_key)
    #         values[name] = bool(result)
    #         pbar.update()
    with open(f"./gmx_config_{pool}.json", "w") as f:
        json.dump(values, f, indent=4)


def query_data_store(func_param, cfg: Cfg, **kwargs):
    if cfg.proxy is None:
        proxy_setting = {}
    else:
        proxy_setting = {"https": cfg.proxy, "http": cfg.proxy}
    agent = Web3(Web3.HTTPProvider(cfg.end_point, request_kwargs={"proxies": proxy_setting}))
    contract = agent.eth.contract(abi=Abis.gmx_data_store_abi, address="0xFD70de6b91282D8017aA4E741e9Ae325CAb992d8")
    func = getattr(contract.functions, func_param)(**kwargs)

    if cfg.height is None:
        cfg.height = "latest"

    result = func.call(block_identifier=cfg.height)
    if cfg.call_back is None:
        return result
    else:
        return cfg.call_back(result)
