import enum
from typing import Tuple, Dict

GMX_FLOAT_DECIMAL=10**30
GM_DECIMAL=10**18

class GmxTopics(enum.StrEnum):
    EventLog = "0x7e3bde2ba7aca4a8499608ca57f3b0c1c1c93ace63ffd3741a9fab204146fc9a"
    EventLog1 = "0x137a44067c8961cd7e1d876f4754a5a3a75989b4552f1843fc69c3b372def160"
    EventLog2 = "0x468a25a7ba624ceea6e540ad6f49171b52495b648417ae91bca21676d8a24dc5"
    OraclePriceUpdate = "0x41c7b30afab659d385f1996d0addfa6e647694862e72378d0b43773f556cbeb2"


data_type = [
    "address",  # sender
    "string",  # eventName
    "("
    "((string,address)[],(string,address[])[]),"
    "((string,uint256)[],(string,uint256[])[]),"
    "((string,int256)[],(string,int256[])[]),"
    "((string,bool)[],(string,bool[])[]),"
    "((string,bytes32)[],(string,bytes32[])[]),"
    "((string,bytes)[],(string,bytes[])[]),"
    "((string,string)[],(string,string[])[])"
    ")",
]
arb_tokens = {
    "0xaf88d065e77c8cc2239327c5edb3a432268e5831": ("USDC", 6),
    "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f": ("WBTC", 8),
    "0x82af49447d8a07e3bd95bd0d56f35241523fbab1": ("WETH", 18),
    "0xfc5a1a6eb076a2c7ad06ed22c90d7e710e35ad0a": ("GMX", 18),
    "0x912ce59144191c1204e64559fe8253a0e49e6548": ("ARB", 18),
    "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9": ("USDT", 6),
    "0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34": ("USDe", 18),
    "0x5979d7b546e38e414f7e9822514be443a4800529": ("wstETH", 18),
    "0xf97f4df75117a78c1a5a0dbb814af92458539fb4": ("LINK", 18),
    "0x2bcc6d6cdbbdc0a4071e48bb3b969b06b3330c07": ("SOL", 9),
    "0x6c84a8f1c29108f47a79964b5fe888d4f4d0de40": ("tBTC", 18),
    "0x565609faf65b92f7be02468acf86f8979423e514": ("WAVAX", 18),
    "0x7f9fbf9bdd3f4105c478b996b648fe6e828a1e98": ("APE", 18),
    "0x0c880f6761f1af8d9aa9c466984b80dab9a8c9e8": ("PENDLE", 18),
    "0xa9004a5421372e1d83fb1f85b0fc986c912f91f3": ("WBNB", 18),
    "0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0": ("UNI", 18),
    "0x606c3e5075e5555e79aa15f1e9facb776f96c248": ("EIGEN", 18),
    "0xa1b91fe9fd52141ff8cac388ce3f10bfdc1de79d": ("$WIF", 6),
    "0xba5ddd1f9d7f570dc94a51479a000e3bce967196": ("AAVE", 18),
    "0xac800fd6159c2a2cb8fc31ef74621eb430287a5a": ("OP", 18),
    "0x25d887ce7a35172c62febfd67a1856f20faebb00": ("PEPE", 18),
    "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8": ("USDC.e", 6),
    "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1": ("DAI", 18),
}


def data_decoder(data: Tuple) -> Dict:
    result = {}
    for i in range(7):
        for j in range(2):
            for item in data[i][j]:
                result[item[0]] = item[1]
    return result

class SwapFeeType(enum.Enum):
    Deposit="0x39226eb4fed85317aa310fa53f734c7af59274c49325ab568f9c4592250e8cc5"
    Withdraw = "0xda1ac8fcb4f900f8ab7c364d553e5b6b8bdc58f74160df840be80995056f3838"
    Swap = "0x7ad0b6f464d338ea140ff9ef891b4a69cf89f107060a105c31bb985d9e532214"