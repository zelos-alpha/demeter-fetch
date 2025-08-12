from typing import NamedTuple

import pandas as pd


class GeneralPoolConfig:
    """
    Every pool has its own value, although the values are the same,
    I'm not sure when they will change.
    """

    maxPnlFactorDeposit: float = 0.9
    maxPnlFactorWithdraw: float = 0.7


class Prices(NamedTuple):
    longPrice: float
    shortPrice: float
    indexPrice: float


def calcPoolValue(row: pd.Series):
    longTokenUsd = row["longAmount"] * row["longPrice"]
    shortTokenUsd = row["shortAmount"] * row["shortPrice"]
    pool_net_value = longTokenUsd + shortTokenUsd

    pool_net_value += row["totalBorrowingFees"] * row["borrowingFeePoolFactor"]

    longPnl = getPnl(row, True)
    longPnl = getCappedPnl(longPnl, longTokenUsd, False)

    shortPnl = getPnl(row, False)
    shortPnl = getCappedPnl(shortPnl, shortTokenUsd, False)
    netPnl = longPnl + shortPnl
    pool_net_value -= netPnl

    impactPoolUsd = row["impactPoolAmount"] * row["indexPrice"]
    pool_net_value -= impactPoolUsd

    return pd.Series([pool_net_value, longPnl, shortPnl, netPnl], index=["poolValue", "longPnl", "shortPnl", "netPnl"])


def getPnl(row: pd.Series, isLong: bool) -> float:
    # When short token == long token, short deposit will be nan
    tmp = (
        row[
            [
                "openInterestShortIsLong",
                "openInterestShortNotLong",
                "openInterestInTokensShortIsLong",
                "openInterestInTokensShortNotLong",
            ]
        ]
        .astype(float)
        .fillna(0)
    )
    openInterest = (
        row["openInterestLongIsLong"] + tmp["openInterestShortIsLong"]
        if isLong
        else row["openInterestLongNotLong"] + tmp["openInterestShortNotLong"]
    )
    openInterestInTokens = (
        row["openInterestInTokensLongIsLong"] + tmp["openInterestInTokensShortIsLong"]
        if isLong
        else row["openInterestInTokensLongNotLong"] + tmp["openInterestInTokensShortNotLong"]
    )
    # openInterest is the cost of all positions, openInterestValue is the current worth of all positions
    openInterestValue = openInterestInTokens * row["indexPrice"]
    pnl = openInterestValue - openInterest if isLong else openInterest - openInterestValue

    return pnl


def getCappedPnl(pnl: float, poolUsd: float, is_deposit: bool) -> float:
    if pnl < 0:
        return pnl
    maxPnlFactor = GeneralPoolConfig.maxPnlFactorDeposit if is_deposit else GeneralPoolConfig.maxPnlFactorWithdraw
    maxPnl = poolUsd * maxPnlFactor

    return maxPnl if pnl > maxPnl else pnl
