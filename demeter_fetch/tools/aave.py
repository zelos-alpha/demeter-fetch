import random
from typing import NamedTuple

import pandas as pd
import requests
from eth_abi import decode

pd.options.display.max_columns = None
pd.set_option("display.width", 5000)

reserve_data_name = [
    "underlyingAsset",
    "name",
    "symbol",
    "decimals",
    "baseLTVasCollateral",
    "reserveLiquidationThreshold",
    "reserveLiquidationBonus",
    "reserveFactor",
    "usageAsCollateralEnabled",
    "borrowingEnabled",
    "isActive",
    "isFrozen",
    "liquidityIndex",
    "variableBorrowIndex",
    "liquidityRate",
    "variableBorrowRate",
    "lastUpdateTimestamp",
    "aTokenAddress",
    "variableDebtTokenAddress",
    "interestRateStrategyAddress",
    "availableLiquidity",
    "totalScaledVariableDebt",
    "priceInMarketReferenceCurrency",
    "priceOracle",
    "variableRateSlope1",
    "variableRateSlope2",
    "baseVariableBorrowRate",
    "optimalUsageRatio",
    "isPaused",
    "isSiloedBorrowing",
    "accruedToTreasury",
    "unbacked",
    "isolationModeTotalDebt",
    "flashLoanEnabled",
    "debtCeiling",
    "debtCeilingDecimals",
    "borrowCap",
    "supplyCap",
    "borrowableInIsolation",
    "virtualAccActive",
    "virtualUnderlyingBalance",
]
base_currency_info = [
    "marketReferenceCurrencyUnit",
    "marketReferenceCurrencyPriceInUsd",
    "networkBaseTokenPriceInUsd",
    "networkBaseTokenPriceDecimals",
]
data_type = [
    "(address,string,string,uint256,uint256,uint256,uint256,uint256,bool,bool,bool,bool,"
    "uint128,uint128,uint128,uint128,uint40,address,address,address,uint256,uint256,uint256,"
    "address,uint256,uint256,uint256,uint256,bool,bool,uint128,uint128,uint128,bool,uint256,"
    "uint256,uint256,uint256,bool,bool,uint128)[]",
    "(uint256,int256,int256,uint8)",
]


class ChainCfg(NamedTuple):
    url: str
    to: str
    data: str


chain_cfgs = {
    "ethereum": ChainCfg(
        "https://eth-mainnet.g.alchemy.com/v2/ZiMMq2478EVIEJdsxC5dMal_ccQwtb31",
        "0x3f78bbd206e4d3c504eb854232eda7e47e9fd8fc",
        "0xec489c210000000000000000000000002f39d218133afab8f2b819b1066c7e434ad94e9e",
    ),
    "polygon": ChainCfg(
        "https://polygon-mainnet.g.alchemy.com/v2/MbgjyHR1CQiU5Y8CUa2mqfRlYwltE5Zr",
        "0x68100bd5345ea474d93577127c11f39ff8463e93",
        "0xec489c21000000000000000000000000a97684ead0e402dc232d5a977953df7ecbab3cdb",
    ),
    "avalanche": ChainCfg(
        "https://avax-mainnet.g.alchemy.com/v2/qBXCF7-6YfiiAdG0dvUyLpQuHt02DbXH",
        "0x50b4a66bf4d41e6252540ea7427d7a933bc3c088",
        "0xec489c21000000000000000000000000a97684ead0e402dc232d5a977953df7ecbab3cdb",
    ),
    "arbitrum": ChainCfg(
        "https://arb-mainnet.g.alchemy.com/v2/2oA-8BGeYqHHpd2uCU49IzeZDL9skdSm",
        "0x5c5228ac8bc1528482514af3e27e692495148717",
        "0xec489c21000000000000000000000000a97684ead0e402dc232d5a977953df7ecbab3cdb",
    ),
    "base": ChainCfg(
        "https://base-mainnet.g.alchemy.com/v2/AFu9kulpkXzHO7kQQ9UQDXWRyEhJEXPk",
        "0x68100bd5345ea474d93577127c11f39ff8463e93",
        "0xec489c21000000000000000000000000e20fcbdbffc4dd138ce8b2e6fbb6cb49777ad64d",
    ),
    "optimism": ChainCfg(
        "https://opt-mainnet.g.alchemy.com/v2/H8ZBGuz1LZbRsYnCBQHY4YMv_AUAVGeM",
        "0xe92cd6164ce7dc68e740765bc1f2a091b6cbc3e4",
        "0xec489c21000000000000000000000000a97684ead0e402dc232d5a977953df7ecbab3cdb",
    ),
    "bsc": ChainCfg(
        "https://bnb-mainnet.g.alchemy.com/v2/nCU1F9Y1KDQFMs9OBtkGw0GLsIKiYBho",
        "0xc0179321f0825c3e0f59fe7ca4e40557b97797a3",
        "0xec489c21000000000000000000000000ff75b6da14ffbbfd355daf7a2731456b3562ba6d",
    ),
    "celo": ChainCfg(
        "https://celo-mainnet.g.alchemy.com/v2/QSIQ93fznmXwv9qEWBnKIOOsQGldk3wL",
        "0xf07ffd12b119b921c4a2ce8d4a13c5d1e3000d6e",
        "0xec489c210000000000000000000000009f7cf9417d5251c59fe94fb9147feee1aad9cea5",
    ),
}


def aave_risk_param(args):
    # https://aave.com/docs/resources/parameters
    cfg: ChainCfg = chain_cfgs[args.chain]
    height = args.block_number
    proxy = args.proxy
    proxies = (
        {
            "http": proxy,
            "https": proxy,
        }
        if proxy
        else {}
    )
    params = [{"to": cfg.to, "data": cfg.data}, "latest" if height == "latest" else hex(int(height))]
    param = {"jsonrpc": "2.0", "method": "eth_call", "params": params, "id": random.randint(1, 2147483648)}
    result = requests.post(cfg.url, json=param, proxies=proxies, headers={"Origin": "https://aave.com"})
    response = result.json()
    data = response["result"]
    decoded_data = decode(data_type, bytes.fromhex(data[2:]))
    token_info_list = []
    for token in decoded_data[0]:
        item_dict = {}
        for item_idx in range(len(token)):
            item_dict[reserve_data_name[item_idx]] = token[item_idx]
        token_info_list.append(item_dict)
    df = pd.DataFrame(token_info_list)

    base_currency_info_dict = {}
    for item_idx in range(len(base_currency_info)):
        base_currency_info_dict[base_currency_info[item_idx]] = decoded_data[1][item_idx]
    print(df)
    df.to_csv(f"./Aave Protocol Parameter {args.chain}.csv",index=False)
    pass
