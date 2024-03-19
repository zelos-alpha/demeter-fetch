from _decimal import Decimal
from typing import List

import numpy as np

import demeter_fetch.common._typing as TYPE
from demeter_fetch.common import split_topic

RAY = 10**27


def decode_event_ReserveDataUpdated(row):
    return (
        Decimal(int(row["data"][2 : (2 + 64)], 16)) / RAY,
        Decimal(int(row["data"][(2 + 64) : (2 + 64 * 2)], 16)) / RAY,
        Decimal(int(row["data"][(2 + 64 * 2) : (2 + 64 * 3)], 16)) / RAY,
        Decimal(int(row["data"][(2 + 64 * 3) : (2 + 64 * 4)], 16)) / RAY,
        Decimal(int(row["data"][(2 + 64 * 4) :], 16)) / RAY,
    )


def hex_to_address(topic_str):
    return "0x" + topic_str[26:]


def signed_int(h):
    """
    Converts hex values to signed integers.
    """
    s = bytes.fromhex(h[2:])
    i = int.from_bytes(s, "big", signed=True)
    return i


def handle_event(tx_type, topics_str, data_hex):
    reserve = None
    owner = None
    amount = None
    debt_asset = None
    debt_amount = Decimal(np.nan)
    liquidator = None
    atoken = None

    topic_list = split_topic(topics_str)
    no_0x_data = data_hex[2:]
    chunk_size = 64
    chunks = len(no_0x_data)
    match tx_type:
        # reserve,
        case TYPE.KECCAK.AAVE_SUPPLY:
            # reserve,onBehalfOf,referralCode,user,amount
            # reserve: trading pool
            # onBehalfOf: supply asset owner
            # referralCode: unique code for 3rd party referral program integration. Use 0 for no referral
            # user: msg sender
            # amount: supplied asset amount
            reserve = hex_to_address(topic_list[1])
            owner = onBehalfOf = hex_to_address(topic_list[2])
            referralCode = signed_int(topic_list[3])
            split_data = ["0x" + no_0x_data[i : i + chunk_size] for i in range(0, chunks, chunk_size)]
            user = hex_to_address(split_data[0])
            amount = signed_int(split_data[1])
        case TYPE.KECCAK.AAVE_WITHDRAW:
            # reserve,user,to,amount
            # reserve: trading pool
            # user: msg sender
            # to: receiver
            # amount: asset amount
            reserve = hex_to_address(topic_list[1])
            user = hex_to_address(topic_list[2])
            owner = to = hex_to_address(topic_list[3])
            amount = signed_int(data_hex)
        case TYPE.KECCAK.AAVE_BORROW:
            # reserve,onBehalfOf,referralCode,user,amount,interestRateMode,borrowRate
            # reserve: trading pool
            # onBehalfOf: borrow asset owner
            # referralCode: refer code
            # user: msg sender
            # amount: borrow asset amount
            # interestRateMode: The rate mode: 1 for Stable, 2 for Variable
            # borrowRate: The numeric rate at which the user has borrowed, expressed in ray
            reserve = hex_to_address(topic_list[1])
            owner = onBehalfOf = hex_to_address(topic_list[2])
            referralCode = signed_int(topic_list[3])
            split_data = ["0x" + no_0x_data[i : i + chunk_size] for i in range(0, chunks, chunk_size)]
            user = hex_to_address(split_data[0])
            amount = signed_int(split_data[1])
            interest_rate_mode = signed_int(split_data[2])
            borrow_rate = signed_int(split_data[3])
        case TYPE.KECCAK.AAVE_REPAY:
            # repayer 还款人, user 债务人
            # reserve,user,repayer,amount,useATokens
            # reserve: trading pool
            # repayer: some body pay directly for repayment
            # user: The beneficiary of the repayment, getting his debt reduced
            # amount: repay amount
            # useATokens: useATokens True if the repayment is done using aTokens, `false` if done with underlying asset directly
            reserve = hex_to_address(topic_list[1])
            owner = user = hex_to_address(topic_list[2])
            repayer = hex_to_address(topic_list[3])
            split_data = ["0x" + no_0x_data[i : i + chunk_size] for i in range(0, chunks, chunk_size)]
            amount = signed_int(split_data[0])
            atoken = signed_int(split_data[1])
        case TYPE.KECCAK.AAVE_LIQUIDATION:
            # liquidator repay debt_asset debt_amount and get collateral_asset liquidated_collateral_amount
            # 清算者购买被清算者一定数量的抵押资产（collateral asset）来偿还被清算者特定资产债务（debt asset）
            # 清算者将用于购买的债务资产转移到atoken，atoken将抵押物或者抵押物对应的atoken转移到清算者，销毁被清算者的一部分债务代币。
            # collateralAsset,debtAsset,user,debtToCover,liquidatedCollateralAmount,liquidator,receiveAToken
            # collateralAsset: The address of the underlying asset used as collateral, to receive as result of the liquidation
            # debtAsset: The address of the underlying borrowed asset to be repaid with the liquidation
            # user: The address of the borrower getting liquidated
            # debtToCover: The debt amount of borrowed `asset` the liquidator wants to cover
            # liquidatedCollateralAmount: The amount of collateral received by the liquidator
            # liquidator: The address of the liquidator
            # receiveAToken: True if the liquidators wants to receive the collateral aTokens, `false` if he wants to receive the underlying collateral asset directly
            split_data = ["0x" + no_0x_data[i : i + chunk_size] for i in range(0, chunks, chunk_size)]
            reserve = collateral_asset = hex_to_address(topic_list[1])  # 清算者想要购买的抵押资产。
            owner = user = hex_to_address(topic_list[3])  # 被清算者
            amount = liquidated_collateral_amount = signed_int(split_data[1])  # 清算者购买的抵押资产数量
            liquidator = hex_to_address(split_data[2])  # 清算者
            debt_asset = hex_to_address(topic_list[2])  # 被清算的债务资产，同时也是清算者要支付的资产。
            debt_amount = debtToCover = signed_int(split_data[0])  # 替被清算者偿还的贷款额
            atoken = signed_int(split_data[3])  # 清算者是否以atoken的形式接收清算得到的抵押资产。
        case _:
            raise ValueError("not support tx type")
    return (reserve, owner, Decimal(amount), liquidator, debt_asset, Decimal(debt_amount), atoken)
