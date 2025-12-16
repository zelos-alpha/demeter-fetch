"""Python translation of the Solidity `Keys` library.

This module reproduces the `keccak256(abi.encode(...))` constants and
key-derivation functions from `contracts/data/Keys.sol` using
`eth_abi` and `eth_utils`.

Usage:
    from scripts import keys
    keys.ACCOUNT_DEPOSIT_LIST  # bytes32 constant
    keys.account_deposit_list_key('0xabc...')  # returns bytes (32 bytes)

Note: install dependencies::
    pip install eth-abi eth-utils
"""
from typing import Union

from eth_abi import encode as abi_encode
from eth_utils import keccak, to_canonical_address


BytesLike = Union[bytes, str]


def _keccak_abi(types, values) -> bytes:
    return keccak(abi_encode(types, values))


def _keccak_bytes(b: bytes) -> bytes:
    return keccak(b)


def _to_address_bytes(addr: BytesLike) -> bytes:
    if isinstance(addr, str):
        return to_canonical_address(addr)
    if isinstance(addr, bytes):
        # Expect 20 bytes
        if len(addr) == 20:
            return addr
        # If passed a 0x-prefixed hex as bytes, try decode
        raise ValueError("address bytes must be 20 bytes")
    raise TypeError("address must be hex string or 20-byte bytes")


# Helper to compute keccak(abi.encode("NAME")) like the Solidity constants
def _const(name: str) -> bytes:
    return _keccak_abi(["string"], [name])


# ----------------------------- Constants -----------------------------
# (translated from Solidity: bytes32 public constant ...)
WNT = _const("WNT")
NONCE = _const("NONCE")

FEE_RECEIVER = _const("FEE_RECEIVER")
HOLDING_ADDRESS = _const("HOLDING_ADDRESS")
RELAY_FEE_ADDRESS = _const("RELAY_FEE_ADDRESS")

MIN_HANDLE_EXECUTION_ERROR_GAS = _const("MIN_HANDLE_EXECUTION_ERROR_GAS")
MIN_HANDLE_EXECUTION_ERROR_GAS_TO_FORWARD = _const("MIN_HANDLE_EXECUTION_ERROR_GAS_TO_FORWARD")
MIN_ADDITIONAL_GAS_FOR_EXECUTION = _const("MIN_ADDITIONAL_GAS_FOR_EXECUTION")

REENTRANCY_GUARD_STATUS = _const("REENTRANCY_GUARD_STATUS")

DEPOSIT_FEE_TYPE = _const("DEPOSIT_FEE_TYPE")
WITHDRAWAL_FEE_TYPE = _const("WITHDRAWAL_FEE_TYPE")
SWAP_FEE_TYPE = _const("SWAP_FEE_TYPE")
ATOMIC_SWAP_FEE_TYPE = _const("ATOMIC_SWAP_FEE_TYPE")
POSITION_FEE_TYPE = _const("POSITION_FEE_TYPE")
UI_DEPOSIT_FEE_TYPE = _const("UI_DEPOSIT_FEE_TYPE")
UI_WITHDRAWAL_FEE_TYPE = _const("UI_WITHDRAWAL_FEE_TYPE")
UI_SWAP_FEE_TYPE = _const("UI_SWAP_FEE_TYPE")
UI_POSITION_FEE_TYPE = _const("UI_POSITION_FEE_TYPE")

UI_FEE_FACTOR = _const("UI_FEE_FACTOR")
MAX_UI_FEE_FACTOR = _const("MAX_UI_FEE_FACTOR")

CLAIMABLE_FEE_AMOUNT = _const("CLAIMABLE_FEE_AMOUNT")
CLAIMABLE_UI_FEE_AMOUNT = _const("CLAIMABLE_UI_FEE_AMOUNT")
MAX_AUTO_CANCEL_ORDERS = _const("MAX_AUTO_CANCEL_ORDERS")
MAX_TOTAL_CALLBACK_GAS_LIMIT_FOR_AUTO_CANCEL_ORDERS = _const(
    "MAX_TOTAL_CALLBACK_GAS_LIMIT_FOR_AUTO_CANCEL_ORDERS"
)

MARKET_LIST = _const("MARKET_LIST")
FEE_BATCH_LIST = _const("FEE_BATCH_LIST")
DEPOSIT_LIST = _const("DEPOSIT_LIST")
ACCOUNT_DEPOSIT_LIST = _const("ACCOUNT_DEPOSIT_LIST")
WITHDRAWAL_LIST = _const("WITHDRAWAL_LIST")
ACCOUNT_WITHDRAWAL_LIST = _const("ACCOUNT_WITHDRAWAL_LIST")
SHIFT_LIST = _const("SHIFT_LIST")
ACCOUNT_SHIFT_LIST = _const("ACCOUNT_SHIFT_LIST")

GLV_LIST = _const("GLV_LIST")
GLV_DEPOSIT_LIST = _const("GLV_DEPOSIT_LIST")
GLV_SHIFT_LIST = _const("GLV_SHIFT_LIST")
ACCOUNT_GLV_DEPOSIT_LIST = _const("ACCOUNT_GLV_DEPOSIT_LIST")
GLV_WITHDRAWAL_LIST = _const("GLV_WITHDRAWAL_LIST")
ACCOUNT_GLV_WITHDRAWAL_LIST = _const("ACCOUNT_GLV_WITHDRAWAL_LIST")
GLV_SUPPORTED_MARKET_LIST = _const("GLV_SUPPORTED_MARKET_LIST")

POSITION_LIST = _const("POSITION_LIST")
ACCOUNT_POSITION_LIST = _const("ACCOUNT_POSITION_LIST")
ORDER_LIST = _const("ORDER_LIST")
ACCOUNT_ORDER_LIST = _const("ACCOUNT_ORDER_LIST")
SUBACCOUNT_LIST = _const("SUBACCOUNT_LIST")
AUTO_CANCEL_ORDER_LIST = _const("AUTO_CANCEL_ORDER_LIST")

IS_MARKET_DISABLED = _const("IS_MARKET_DISABLED")
MAX_SWAP_PATH_LENGTH = _const("MAX_SWAP_PATH_LENGTH")
SWAP_PATH_MARKET_FLAG = _const("SWAP_PATH_MARKET_FLAG")
MIN_MARKET_TOKENS_FOR_FIRST_DEPOSIT = _const("MIN_MARKET_TOKENS_FOR_FIRST_DEPOSIT")

CREATE_GLV_DEPOSIT_FEATURE_DISABLED = _const("CREATE_GLV_DEPOSIT_FEATURE_DISABLED")
CANCEL_GLV_DEPOSIT_FEATURE_DISABLED = _const("CANCEL_GLV_DEPOSIT_FEATURE_DISABLED")
EXECUTE_GLV_DEPOSIT_FEATURE_DISABLED = _const("EXECUTE_GLV_DEPOSIT_FEATURE_DISABLED")
CREATE_GLV_WITHDRAWAL_FEATURE_DISABLED = _const("CREATE_GLV_WITHDRAWAL_FEATURE_DISABLED")
CANCEL_GLV_WITHDRAWAL_FEATURE_DISABLED = _const("CANCEL_GLV_WITHDRAWAL_FEATURE_DISABLED")
EXECUTE_GLV_WITHDRAWAL_FEATURE_DISABLED = _const("EXECUTE_GLV_WITHDRAWAL_FEATURE_DISABLED")
CREATE_GLV_SHIFT_FEATURE_DISABLED = _const("CREATE_GLV_SHIFT_FEATURE_DISABLED")
EXECUTE_GLV_SHIFT_FEATURE_DISABLED = _const("EXECUTE_GLV_SHIFT_FEATURE_DISABLED")
JIT_FEATURE_DISABLED = _const("JIT_FEATURE_DISABLED")

CREATE_DEPOSIT_FEATURE_DISABLED = _const("CREATE_DEPOSIT_FEATURE_DISABLED")
CANCEL_DEPOSIT_FEATURE_DISABLED = _const("CANCEL_DEPOSIT_FEATURE_DISABLED")
EXECUTE_DEPOSIT_FEATURE_DISABLED = _const("EXECUTE_DEPOSIT_FEATURE_DISABLED")
CREATE_WITHDRAWAL_FEATURE_DISABLED = _const("CREATE_WITHDRAWAL_FEATURE_DISABLED")
CANCEL_WITHDRAWAL_FEATURE_DISABLED = _const("CANCEL_WITHDRAWAL_FEATURE_DISABLED")
EXECUTE_WITHDRAWAL_FEATURE_DISABLED = _const("EXECUTE_WITHDRAWAL_FEATURE_DISABLED")
EXECUTE_ATOMIC_WITHDRAWAL_FEATURE_DISABLED = _const("EXECUTE_ATOMIC_WITHDRAWAL_FEATURE_DISABLED")

CREATE_SHIFT_FEATURE_DISABLED = _const("CREATE_SHIFT_FEATURE_DISABLED")
CANCEL_SHIFT_FEATURE_DISABLED = _const("CANCEL_SHIFT_FEATURE_DISABLED")
EXECUTE_SHIFT_FEATURE_DISABLED = _const("EXECUTE_SHIFT_FEATURE_DISABLED")

CREATE_ORDER_FEATURE_DISABLED = _const("CREATE_ORDER_FEATURE_DISABLED")
EXECUTE_ORDER_FEATURE_DISABLED = _const("EXECUTE_ORDER_FEATURE_DISABLED")
EXECUTE_ADL_FEATURE_DISABLED = _const("EXECUTE_ADL_FEATURE_DISABLED")
UPDATE_ORDER_FEATURE_DISABLED = _const("UPDATE_ORDER_FEATURE_DISABLED")
CANCEL_ORDER_FEATURE_DISABLED = _const("CANCEL_ORDER_FEATURE_DISABLED")

CLAIM_FUNDING_FEES_FEATURE_DISABLED = _const("CLAIM_FUNDING_FEES_FEATURE_DISABLED")
CLAIM_COLLATERAL_FEATURE_DISABLED = _const("CLAIM_COLLATERAL_FEATURE_DISABLED")
CLAIM_AFFILIATE_REWARDS_FEATURE_DISABLED = _const("CLAIM_AFFILIATE_REWARDS_FEATURE_DISABLED")
CLAIM_UI_FEES_FEATURE_DISABLED = _const("CLAIM_UI_FEES_FEATURE_DISABLED")
SUBACCOUNT_FEATURE_DISABLED = _const("SUBACCOUNT_FEATURE_DISABLED")
GASLESS_FEATURE_DISABLED = _const("GASLESS_FEATURE_DISABLED")
GENERAL_CLAIM_FEATURE_DISABLED = _const("GENERAL_CLAIM_FEATURE_DISABLED")

MIN_ORACLE_SIGNERS = _const("MIN_ORACLE_SIGNERS")
MIN_ORACLE_BLOCK_CONFIRMATIONS = _const("MIN_ORACLE_BLOCK_CONFIRMATIONS")
MAX_ORACLE_PRICE_AGE = _const("MAX_ORACLE_PRICE_AGE")
MAX_ATOMIC_ORACLE_PRICE_AGE = _const("MAX_ATOMIC_ORACLE_PRICE_AGE")
MAX_ORACLE_TIMESTAMP_RANGE = _const("MAX_ORACLE_TIMESTAMP_RANGE")
MAX_ORACLE_REF_PRICE_DEVIATION_FACTOR = _const("MAX_ORACLE_REF_PRICE_DEVIATION_FACTOR")
IS_ORACLE_PROVIDER_ENABLED = _const("IS_ORACLE_PROVIDER_ENABLED")
IS_ATOMIC_ORACLE_PROVIDER = _const("IS_ATOMIC_ORACLE_PROVIDER")
ORACLE_TIMESTAMP_ADJUSTMENT = _const("ORACLE_TIMESTAMP_ADJUSTMENT")
ORACLE_PROVIDER_FOR_TOKEN = _const("ORACLE_PROVIDER_FOR_TOKEN")
ORACLE_PROVIDER_UPDATED_AT = _const("ORACLE_PROVIDER_UPDATED_AT")
ORACLE_PROVIDER_MIN_CHANGE_DELAY = _const("ORACLE_PROVIDER_MIN_CHANGE_DELAY")
CHAINLINK_PAYMENT_TOKEN = _const("CHAINLINK_PAYMENT_TOKEN")
SEQUENCER_GRACE_DURATION = _const("SEQUENCER_GRACE_DURATION")

POSITION_FEE_RECEIVER_FACTOR = _const("POSITION_FEE_RECEIVER_FACTOR")
LIQUIDATION_FEE_RECEIVER_FACTOR = _const("LIQUIDATION_FEE_RECEIVER_FACTOR")
SWAP_FEE_RECEIVER_FACTOR = _const("SWAP_FEE_RECEIVER_FACTOR")
BORROWING_FEE_RECEIVER_FACTOR = _const("BORROWING_FEE_RECEIVER_FACTOR")

ESTIMATED_GAS_FEE_BASE_AMOUNT_V2_1 = _const("ESTIMATED_GAS_FEE_BASE_AMOUNT_V2_1")
ESTIMATED_GAS_FEE_PER_ORACLE_PRICE = _const("ESTIMATED_GAS_FEE_PER_ORACLE_PRICE")
ESTIMATED_GAS_FEE_MULTIPLIER_FACTOR = _const("ESTIMATED_GAS_FEE_MULTIPLIER_FACTOR")

EXECUTION_GAS_FEE_BASE_AMOUNT_V2_1 = _const("EXECUTION_GAS_FEE_BASE_AMOUNT_V2_1")
EXECUTION_GAS_FEE_PER_ORACLE_PRICE = _const("EXECUTION_GAS_FEE_PER_ORACLE_PRICE")
EXECUTION_GAS_FEE_MULTIPLIER_FACTOR = _const("EXECUTION_GAS_FEE_MULTIPLIER_FACTOR")

MAX_EXECUTION_FEE_MULTIPLIER_FACTOR = _const("MAX_EXECUTION_FEE_MULTIPLIER_FACTOR")

MAX_RELAY_FEE_SWAP_USD_FOR_SUBACCOUNT = _const("MAX_RELAY_FEE_SWAP_USD_FOR_SUBACCOUNT")
GELATO_RELAY_FEE_MULTIPLIER_FACTOR = _const("GELATO_RELAY_FEE_MULTIPLIER_FACTOR")
GELATO_RELAY_FEE_BASE_AMOUNT = _const("GELATO_RELAY_FEE_BASE_AMOUNT")
CREATE_DEPOSIT_GAS_LIMIT = _const("CREATE_DEPOSIT_GAS_LIMIT")
DEPOSIT_GAS_LIMIT = _const("DEPOSIT_GAS_LIMIT")
CREATE_WITHDRAWAL_GAS_LIMIT = _const("CREATE_WITHDRAWAL_GAS_LIMIT")
WITHDRAWAL_GAS_LIMIT = _const("WITHDRAWAL_GAS_LIMIT")
CREATE_GLV_DEPOSIT_GAS_LIMIT = _const("CREATE_GLV_DEPOSIT_GAS_LIMIT")
GLV_DEPOSIT_GAS_LIMIT = _const("GLV_DEPOSIT_GAS_LIMIT")
CREATE_GLV_WITHDRAWAL_GAS_LIMIT = _const("CREATE_GLV_WITHDRAWAL_GAS_LIMIT")
GLV_WITHDRAWAL_GAS_LIMIT = _const("GLV_WITHDRAWAL_GAS_LIMIT")
GLV_SHIFT_GAS_LIMIT = _const("GLV_SHIFT_GAS_LIMIT")
GLV_PER_MARKET_GAS_LIMIT = _const("GLV_PER_MARKET_GAS_LIMIT")
SHIFT_GAS_LIMIT = _const("SHIFT_GAS_LIMIT")
SINGLE_SWAP_GAS_LIMIT = _const("SINGLE_SWAP_GAS_LIMIT")
INCREASE_ORDER_GAS_LIMIT = _const("INCREASE_ORDER_GAS_LIMIT")
DECREASE_ORDER_GAS_LIMIT = _const("DECREASE_ORDER_GAS_LIMIT")
SWAP_ORDER_GAS_LIMIT = _const("SWAP_ORDER_GAS_LIMIT")
SET_TRADER_REFERRAL_CODE_GAS_LIMIT = _const("SET_TRADER_REFERRAL_CODE_GAS_LIMIT")
REGISTER_CODE_GAS_LIMIT = _const("REGISTER_CODE_GAS_LIMIT")

TOKEN_TRANSFER_GAS_LIMIT = _const("TOKEN_TRANSFER_GAS_LIMIT")
NATIVE_TOKEN_TRANSFER_GAS_LIMIT = _const("NATIVE_TOKEN_TRANSFER_GAS_LIMIT")

REQUEST_EXPIRATION_TIME = _const("REQUEST_EXPIRATION_TIME")

MAX_CALLBACK_GAS_LIMIT = _const("MAX_CALLBACK_GAS_LIMIT")
REFUND_EXECUTION_FEE_GAS_LIMIT = _const("REFUND_EXECUTION_FEE_GAS_LIMIT")
SAVED_CALLBACK_CONTRACT = _const("SAVED_CALLBACK_CONTRACT")

MIN_COLLATERAL_FACTOR = _const("MIN_COLLATERAL_FACTOR")
MIN_COLLATERAL_FACTOR_FOR_OPEN_INTEREST_MULTIPLIER = _const(
    "MIN_COLLATERAL_FACTOR_FOR_OPEN_INTEREST_MULTIPLIER"
)
MIN_COLLATERAL_USD = _const("MIN_COLLATERAL_USD")
MIN_COLLATERAL_FACTOR_FOR_LIQUIDATION = _const("MIN_COLLATERAL_FACTOR_FOR_LIQUIDATION")
MIN_POSITION_SIZE_USD = _const("MIN_POSITION_SIZE_USD")

VIRTUAL_TOKEN_ID = _const("VIRTUAL_TOKEN_ID")
VIRTUAL_MARKET_ID = _const("VIRTUAL_MARKET_ID")
VIRTUAL_INVENTORY_FOR_SWAPS = _const("VIRTUAL_INVENTORY_FOR_SWAPS")
VIRTUAL_INVENTORY_FOR_POSITIONS = _const("VIRTUAL_INVENTORY_FOR_POSITIONS")
VIRTUAL_INVENTORY_FOR_POSITIONS_IN_TOKENS = _const("VIRTUAL_INVENTORY_FOR_POSITIONS_IN_TOKENS")

POSITION_IMPACT_FACTOR = _const("POSITION_IMPACT_FACTOR")
POSITION_IMPACT_EXPONENT_FACTOR = _const("POSITION_IMPACT_EXPONENT_FACTOR")
MAX_POSITION_IMPACT_FACTOR = _const("MAX_POSITION_IMPACT_FACTOR")
MAX_POSITION_IMPACT_FACTOR_FOR_LIQUIDATIONS = _const(
    "MAX_POSITION_IMPACT_FACTOR_FOR_LIQUIDATIONS"
)
POSITION_FEE_FACTOR = _const("POSITION_FEE_FACTOR")
PRO_TRADER_TIER = _const("PRO_TRADER_TIER")
PRO_DISCOUNT_FACTOR = _const("PRO_DISCOUNT_FACTOR")
LIQUIDATION_FEE_FACTOR = _const("LIQUIDATION_FEE_FACTOR")
SWAP_IMPACT_FACTOR = _const("SWAP_IMPACT_FACTOR")
SWAP_IMPACT_EXPONENT_FACTOR = _const("SWAP_IMPACT_EXPONENT_FACTOR")
SWAP_FEE_FACTOR = _const("SWAP_FEE_FACTOR")
ATOMIC_SWAP_FEE_FACTOR = _const("ATOMIC_SWAP_FEE_FACTOR")
ATOMIC_WITHDRAWAL_FEE_FACTOR = _const("ATOMIC_WITHDRAWAL_FEE_FACTOR")
DEPOSIT_FEE_FACTOR = _const("DEPOSIT_FEE_FACTOR")
WITHDRAWAL_FEE_FACTOR = _const("WITHDRAWAL_FEE_FACTOR")
ORACLE_TYPE = _const("ORACLE_TYPE")
OPEN_INTEREST = _const("OPEN_INTEREST")
OPEN_INTEREST_IN_TOKENS = _const("OPEN_INTEREST_IN_TOKENS")
COLLATERAL_SUM = _const("COLLATERAL_SUM")
MAX_COLLATERAL_SUM = _const("MAX_COLLATERAL_SUM")
POOL_AMOUNT = _const("POOL_AMOUNT")
MAX_POOL_AMOUNT = _const("MAX_POOL_AMOUNT")
MAX_POOL_USD_FOR_DEPOSIT = _const("MAX_POOL_USD_FOR_DEPOSIT")
MAX_OPEN_INTEREST = _const("MAX_OPEN_INTEREST")
POSITION_IMPACT_POOL_AMOUNT = _const("POSITION_IMPACT_POOL_AMOUNT")
LENT_POSITION_IMPACT_POOL_AMOUNT = _const("LENT_POSITION_IMPACT_POOL_AMOUNT")
MAX_LENDABLE_IMPACT_FACTOR = _const("MAX_LENDABLE_IMPACT_FACTOR")
MAX_LENDABLE_IMPACT_FACTOR_FOR_WITHDRAWALS = _const("MAX_LENDABLE_IMPACT_FACTOR_FOR_WITHDRAWALS")
MAX_LENDABLE_IMPACT_USD = _const("MAX_LENDABLE_IMPACT_USD")
TOTAL_PENDING_IMPACT_AMOUNT = _const("TOTAL_PENDING_IMPACT_AMOUNT")
MIN_POSITION_IMPACT_POOL_AMOUNT = _const("MIN_POSITION_IMPACT_POOL_AMOUNT")
POSITION_IMPACT_POOL_DISTRIBUTION_RATE = _const("POSITION_IMPACT_POOL_DISTRIBUTION_RATE")
POSITION_IMPACT_POOL_DISTRIBUTED_AT = _const("POSITION_IMPACT_POOL_DISTRIBUTED_AT")
SWAP_IMPACT_POOL_AMOUNT = _const("SWAP_IMPACT_POOL_AMOUNT")
PRICE_FEED = _const("PRICE_FEED")
PRICE_FEED_MULTIPLIER = _const("PRICE_FEED_MULTIPLIER")
PRICE_FEED_HEARTBEAT_DURATION = _const("PRICE_FEED_HEARTBEAT_DURATION")
DATA_STREAM_ID = _const("DATA_STREAM_ID")
EDGE_DATA_STREAM_ID = _const("EDGE_DATA_STREAM_ID")
EDGE_DATA_STREAM_TOKEN_DECIMALS = _const("EDGE_DATA_STREAM_TOKEN_DECIMALS")
DATA_STREAM_MULTIPLIER = _const("DATA_STREAM_MULTIPLIER")
DATA_STREAM_SPREAD_REDUCTION_FACTOR = _const("DATA_STREAM_SPREAD_REDUCTION_FACTOR")
STABLE_PRICE = _const("STABLE_PRICE")
RESERVE_FACTOR = _const("RESERVE_FACTOR")
OPEN_INTEREST_RESERVE_FACTOR = _const("OPEN_INTEREST_RESERVE_FACTOR")
MAX_PNL_FACTOR = _const("MAX_PNL_FACTOR")
MAX_PNL_FACTOR_FOR_TRADERS = _const("MAX_PNL_FACTOR_FOR_TRADERS")
MAX_PNL_FACTOR_FOR_ADL = _const("MAX_PNL_FACTOR_FOR_ADL")
MIN_PNL_FACTOR_AFTER_ADL = _const("MIN_PNL_FACTOR_AFTER_ADL")
MAX_PNL_FACTOR_FOR_DEPOSITS = _const("MAX_PNL_FACTOR_FOR_DEPOSITS")
MAX_PNL_FACTOR_FOR_WITHDRAWALS = _const("MAX_PNL_FACTOR_FOR_WITHDRAWALS")
LATEST_ADL_AT = _const("LATEST_ADL_AT")
IS_ADL_ENABLED = _const("IS_ADL_ENABLED")
FUNDING_FACTOR = _const("FUNDING_FACTOR")
FUNDING_EXPONENT_FACTOR = _const("FUNDING_EXPONENT_FACTOR")
SAVED_FUNDING_FACTOR_PER_SECOND = _const("SAVED_FUNDING_FACTOR_PER_SECOND")
FUNDING_INCREASE_FACTOR_PER_SECOND = _const("FUNDING_INCREASE_FACTOR_PER_SECOND")
FUNDING_DECREASE_FACTOR_PER_SECOND = _const("FUNDING_DECREASE_FACTOR_PER_SECOND")
MIN_FUNDING_FACTOR_PER_SECOND = _const("MIN_FUNDING_FACTOR_PER_SECOND")
MAX_FUNDING_FACTOR_PER_SECOND = _const("MAX_FUNDING_FACTOR_PER_SECOND")
THRESHOLD_FOR_STABLE_FUNDING = _const("THRESHOLD_FOR_STABLE_FUNDING")
THRESHOLD_FOR_DECREASE_FUNDING = _const("THRESHOLD_FOR_DECREASE_FUNDING")
FUNDING_FEE_AMOUNT_PER_SIZE = _const("FUNDING_FEE_AMOUNT_PER_SIZE")
CLAIMABLE_FUNDING_AMOUNT_PER_SIZE = _const("CLAIMABLE_FUNDING_AMOUNT_PER_SIZE")
FUNDING_UPDATED_AT = _const("FUNDING_UPDATED_AT")
CLAIMABLE_FUNDING_AMOUNT = _const("CLAIMABLE_FUNDING_AMOUNT")
CLAIMABLE_COLLATERAL_AMOUNT = _const("CLAIMABLE_COLLATERAL_AMOUNT")
CLAIMABLE_COLLATERAL_FACTOR = _const("CLAIMABLE_COLLATERAL_FACTOR")
CLAIMABLE_COLLATERAL_REDUCTION_FACTOR = _const("CLAIMABLE_COLLATERAL_REDUCTION_FACTOR")
CLAIMABLE_COLLATERAL_TIME_DIVISOR = _const("CLAIMABLE_COLLATERAL_TIME_DIVISOR")
CLAIMABLE_COLLATERAL_DELAY = _const("CLAIMABLE_COLLATERAL_DELAY")
CLAIMED_COLLATERAL_AMOUNT = _const("CLAIMED_COLLATERAL_AMOUNT")

OPTIMAL_USAGE_FACTOR = _const("OPTIMAL_USAGE_FACTOR")
BASE_BORROWING_FACTOR = _const("BASE_BORROWING_FACTOR")
ABOVE_OPTIMAL_USAGE_BORROWING_FACTOR = _const("ABOVE_OPTIMAL_USAGE_BORROWING_FACTOR")
BORROWING_FACTOR = _const("BORROWING_FACTOR")
BORROWING_EXPONENT_FACTOR = _const("BORROWING_EXPONENT_FACTOR")
SKIP_BORROWING_FEE_FOR_SMALLER_SIDE = _const("SKIP_BORROWING_FEE_FOR_SMALLER_SIDE")
CUMULATIVE_BORROWING_FACTOR = _const("CUMULATIVE_BORROWING_FACTOR")
CUMULATIVE_BORROWING_FACTOR_UPDATED_AT = _const("CUMULATIVE_BORROWING_FACTOR_UPDATED_AT")
TOTAL_BORROWING = _const("TOTAL_BORROWING")

USE_OPEN_INTEREST_IN_TOKENS_FOR_BALANCE = _const("USE_OPEN_INTEREST_IN_TOKENS_FOR_BALANCE")

MIN_AFFILIATE_REWARD_FACTOR = _const("MIN_AFFILIATE_REWARD_FACTOR")
AFFILIATE_REWARD = _const("AFFILIATE_REWARD")
MAX_ALLOWED_SUBACCOUNT_ACTION_COUNT = _const("MAX_ALLOWED_SUBACCOUNT_ACTION_COUNT")
SUBACCOUNT_EXPIRES_AT = _const("SUBACCOUNT_EXPIRES_AT")
SUBACCOUNT_ACTION_COUNT = _const("SUBACCOUNT_ACTION_COUNT")
SUBACCOUNT_AUTO_TOP_UP_AMOUNT = _const("SUBACCOUNT_AUTO_TOP_UP_AMOUNT")
SUBACCOUNT_ORDER_ACTION = _const("SUBACCOUNT_ORDER_ACTION")
SUBACCOUNT_INTEGRATION_ID = _const("SUBACCOUNT_INTEGRATION_ID")
SUBACCOUNT_INTEGRATION_DISABLED = _const("SUBACCOUNT_INTEGRATION_DISABLED")
FEE_DISTRIBUTOR_SWAP_TOKEN_INDEX = _const("FEE_DISTRIBUTOR_SWAP_TOKEN_INDEX")
FEE_DISTRIBUTOR_SWAP_FEE_BATCH = _const("FEE_DISTRIBUTOR_SWAP_FEE_BATCH")

GLV_MAX_MARKET_COUNT = _const("GLV_MAX_MARKET_COUNT")
GLV_MAX_MARKET_TOKEN_BALANCE_USD = _const("GLV_MAX_MARKET_TOKEN_BALANCE_USD")
GLV_MAX_MARKET_TOKEN_BALANCE_AMOUNT = _const("GLV_MAX_MARKET_TOKEN_BALANCE_AMOUNT")
IS_GLV_MARKET_DISABLED = _const("IS_GLV_MARKET_DISABLED")
GLV_SHIFT_MAX_LOSS_FACTOR = _const("GLV_SHIFT_MAX_LOSS_FACTOR")
GLV_SHIFT_LAST_EXECUTED_AT = _const("GLV_SHIFT_LAST_EXECUTED_AT")
GLV_SHIFT_MIN_INTERVAL = _const("GLV_SHIFT_MIN_INTERVAL")
MIN_GLV_TOKENS_FOR_FIRST_DEPOSIT = _const("MIN_GLV_TOKENS_FOR_FIRST_DEPOSIT")

SYNC_CONFIG_FEATURE_DISABLED = _const("SYNC_CONFIG_FEATURE_DISABLED")
SYNC_CONFIG_MARKET_DISABLED = _const("SYNC_CONFIG_MARKET_DISABLED")
SYNC_CONFIG_PARAMETER_DISABLED = _const("SYNC_CONFIG_PARAMETER_DISABLED")
SYNC_CONFIG_MARKET_PARAMETER_DISABLED = _const("SYNC_CONFIG_MARKET_PARAMETER_DISABLED")
SYNC_CONFIG_UPDATE_COMPLETED = _const("SYNC_CONFIG_UPDATE_COMPLETED")
SYNC_CONFIG_LATEST_UPDATE_ID = _const("SYNC_CONFIG_LATEST_UPDATE_ID")

CONTRIBUTOR_ACCOUNT_LIST = _const("CONTRIBUTOR_ACCOUNT_LIST")
CONTRIBUTOR_TOKEN_LIST = _const("CONTRIBUTOR_TOKEN_LIST")
CONTRIBUTOR_TOKEN_AMOUNT = _const("CONTRIBUTOR_TOKEN_AMOUNT")
MAX_TOTAL_CONTRIBUTOR_TOKEN_AMOUNT = _const("MAX_TOTAL_CONTRIBUTOR_TOKEN_AMOUNT")
CONTRIBUTOR_FUNDING_ACCOUNT = _const("CONTRIBUTOR_FUNDING_ACCOUNT")
CUSTOM_CONTRIBUTOR_FUNDING_ACCOUNT = _const("CUSTOM_CONTRIBUTOR_FUNDING_ACCOUNT")
CONTRIBUTOR_LAST_PAYMENT_AT = _const("CONTRIBUTOR_LAST_PAYMENT_AT")
MIN_CONTRIBUTOR_PAYMENT_INTERVAL = _const("MIN_CONTRIBUTOR_PAYMENT_INTERVAL")

BUYBACK_BATCH_AMOUNT = _const("BUYBACK_BATCH_AMOUNT")
BUYBACK_AVAILABLE_FEE_AMOUNT = _const("BUYBACK_AVAILABLE_FEE_AMOUNT")
BUYBACK_GMX_FACTOR = _const("BUYBACK_GMX_FACTOR")
BUYBACK_MAX_PRICE_IMPACT_FACTOR = _const("BUYBACK_MAX_PRICE_IMPACT_FACTOR")
BUYBACK_MAX_PRICE_AGE = _const("BUYBACK_MAX_PRICE_AGE")
WITHDRAWABLE_BUYBACK_TOKEN_AMOUNT = _const("WITHDRAWABLE_BUYBACK_TOKEN_AMOUNT")

MULTICHAIN_BALANCE = _const("MULTICHAIN_BALANCE")
IS_MULTICHAIN_PROVIDER_ENABLED = _const("IS_MULTICHAIN_PROVIDER_ENABLED")
IS_MULTICHAIN_ENDPOINT_ENABLED = _const("IS_MULTICHAIN_ENDPOINT_ENABLED")
IS_RELAY_FEE_EXCLUDED = _const("IS_RELAY_FEE_EXCLUDED")
IS_SRC_CHAIN_ID_ENABLED = _const("IS_SRC_CHAIN_ID_ENABLED")
POSITION_LAST_SRC_CHAIN_ID = _const("POSITION_LAST_SRC_CHAIN_ID")
EID_TO_SRC_CHAIN_ID = _const("EID_TO_SRC_CHAIN_ID")

MAX_DATA_LENGTH = _const("MAX_DATA_LENGTH")
GMX_DATA_ACTION = _const("GMX_DATA_ACTION")
CLAIMABLE_FUNDS_AMOUNT = _const("CLAIMABLE_FUNDS_AMOUNT")
TOTAL_CLAIMABLE_FUNDS_AMOUNT = _const("TOTAL_CLAIMABLE_FUNDS_AMOUNT")
CLAIM_TERMS = _const("CLAIM_TERMS")
CLAIM_TERMS_BACKREF = _const("CLAIM_TERMS_BACKREF")

USER_INITIATED_CANCEL = "USER_INITIATED_CANCEL"


# ----------------------------- Functions -----------------------------
def get_full_key(base_key: bytes, data: bytes) -> bytes:
    """If data is empty return base_key else keccak(base_key + data)."""
    if not data:
        return base_key
    return _keccak_bytes(base_key + data)


def account_deposit_list_key(account: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [ACCOUNT_DEPOSIT_LIST, _to_address_bytes(account)])


def account_withdrawal_list_key(account: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [ACCOUNT_WITHDRAWAL_LIST, _to_address_bytes(account)])


def account_shift_list_key(account: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [ACCOUNT_SHIFT_LIST, _to_address_bytes(account)])


def account_glv_deposit_list_key(account: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [ACCOUNT_GLV_DEPOSIT_LIST, _to_address_bytes(account)])


def account_glv_withdrawal_list_key(account: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [ACCOUNT_GLV_WITHDRAWAL_LIST, _to_address_bytes(account)])


def glv_supported_market_list_key(glv: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [GLV_SUPPORTED_MARKET_LIST, _to_address_bytes(glv)])


def account_position_list_key(account: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [ACCOUNT_POSITION_LIST, _to_address_bytes(account)])


def account_order_list_key(account: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [ACCOUNT_ORDER_LIST, _to_address_bytes(account)])


def subaccount_list_key(account: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [SUBACCOUNT_LIST, _to_address_bytes(account)])


def auto_cancel_order_list_key(position_key: bytes) -> bytes:
    return _keccak_abi(["bytes32", "bytes32"], [AUTO_CANCEL_ORDER_LIST, position_key])


def claimable_fee_amount_key(market: BytesLike, token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address", "address"], [CLAIMABLE_FEE_AMOUNT, _to_address_bytes(market), _to_address_bytes(token)])


def claimable_ui_fee_amount_key(market: BytesLike, token: BytesLike, account: BytesLike = None) -> bytes:
    if account is None:
        return _keccak_abi(["bytes32", "address", "address"], [CLAIMABLE_UI_FEE_AMOUNT, _to_address_bytes(market), _to_address_bytes(token)])
    return _keccak_abi(["bytes32", "address", "address", "address"], [CLAIMABLE_UI_FEE_AMOUNT, _to_address_bytes(market), _to_address_bytes(token), _to_address_bytes(account)])


def deposit_gas_limit_key() -> bytes:
    return DEPOSIT_GAS_LIMIT


def withdrawal_gas_limit_key() -> bytes:
    return WITHDRAWAL_GAS_LIMIT


def shift_gas_limit_key() -> bytes:
    return SHIFT_GAS_LIMIT


def glv_deposit_gas_limit_key() -> bytes:
    return GLV_DEPOSIT_GAS_LIMIT


def glv_withdrawal_gas_limit_key() -> bytes:
    return GLV_WITHDRAWAL_GAS_LIMIT


def glv_shift_gas_limit_key() -> bytes:
    return GLV_SHIFT_GAS_LIMIT


def glv_per_market_gas_limit_key() -> bytes:
    return GLV_PER_MARKET_GAS_LIMIT


def single_swap_gas_limit_key() -> bytes:
    return SINGLE_SWAP_GAS_LIMIT


def increase_order_gas_limit_key() -> bytes:
    return INCREASE_ORDER_GAS_LIMIT


def decrease_order_gas_limit_key() -> bytes:
    return DECREASE_ORDER_GAS_LIMIT


def swap_order_gas_limit_key() -> bytes:
    return SWAP_ORDER_GAS_LIMIT


def swap_path_market_flag_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [SWAP_PATH_MARKET_FLAG, _to_address_bytes(market)])


def create_glv_deposit_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [CREATE_GLV_DEPOSIT_FEATURE_DISABLED, _to_address_bytes(module)])


def cancel_glv_deposit_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [CANCEL_GLV_DEPOSIT_FEATURE_DISABLED, _to_address_bytes(module)])


def execute_glv_deposit_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [EXECUTE_GLV_DEPOSIT_FEATURE_DISABLED, _to_address_bytes(module)])


def create_glv_withdrawal_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [CREATE_GLV_WITHDRAWAL_FEATURE_DISABLED, _to_address_bytes(module)])


def cancel_glv_withdrawal_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [CANCEL_GLV_WITHDRAWAL_FEATURE_DISABLED, _to_address_bytes(module)])


def execute_glv_withdrawal_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [EXECUTE_GLV_WITHDRAWAL_FEATURE_DISABLED, _to_address_bytes(module)])


def create_glv_shift_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [CREATE_GLV_SHIFT_FEATURE_DISABLED, _to_address_bytes(module)])


def execute_glv_shift_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [EXECUTE_GLV_SHIFT_FEATURE_DISABLED, _to_address_bytes(module)])


def jit_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [JIT_FEATURE_DISABLED, _to_address_bytes(module)])


def create_deposit_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [CREATE_DEPOSIT_FEATURE_DISABLED, _to_address_bytes(module)])


def cancel_deposit_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [CANCEL_DEPOSIT_FEATURE_DISABLED, _to_address_bytes(module)])


def execute_deposit_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [EXECUTE_DEPOSIT_FEATURE_DISABLED, _to_address_bytes(module)])


def create_withdrawal_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [CREATE_WITHDRAWAL_FEATURE_DISABLED, _to_address_bytes(module)])


def cancel_withdrawal_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [CANCEL_WITHDRAWAL_FEATURE_DISABLED, _to_address_bytes(module)])


def execute_withdrawal_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [EXECUTE_WITHDRAWAL_FEATURE_DISABLED, _to_address_bytes(module)])


def execute_atomic_withdrawal_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [EXECUTE_ATOMIC_WITHDRAWAL_FEATURE_DISABLED, _to_address_bytes(module)])


def create_shift_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [CREATE_SHIFT_FEATURE_DISABLED, _to_address_bytes(module)])


def cancel_shift_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [CANCEL_SHIFT_FEATURE_DISABLED, _to_address_bytes(module)])


def execute_shift_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [EXECUTE_SHIFT_FEATURE_DISABLED, _to_address_bytes(module)])


def create_order_feature_disabled_key(module: BytesLike, order_type: int) -> bytes:
    return _keccak_abi(["bytes32", "address", "uint256"], [CREATE_ORDER_FEATURE_DISABLED, _to_address_bytes(module), order_type])


def execute_order_feature_disabled_key(module: BytesLike, order_type: int) -> bytes:
    return _keccak_abi(["bytes32", "address", "uint256"], [EXECUTE_ORDER_FEATURE_DISABLED, _to_address_bytes(module), order_type])


def execute_adl_feature_disabled_key(module: BytesLike, order_type: int) -> bytes:
    return _keccak_abi(["bytes32", "address", "uint256"], [EXECUTE_ADL_FEATURE_DISABLED, _to_address_bytes(module), order_type])


def update_order_feature_disabled_key(module: BytesLike, order_type: int) -> bytes:
    return _keccak_abi(["bytes32", "address", "uint256"], [UPDATE_ORDER_FEATURE_DISABLED, _to_address_bytes(module), order_type])


def cancel_order_feature_disabled_key(module: BytesLike, order_type: int) -> bytes:
    return _keccak_abi(["bytes32", "address", "uint256"], [CANCEL_ORDER_FEATURE_DISABLED, _to_address_bytes(module), order_type])


def claim_funding_fees_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [CLAIM_FUNDING_FEES_FEATURE_DISABLED, _to_address_bytes(module)])


def claim_collateral_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [CLAIM_COLLATERAL_FEATURE_DISABLED, _to_address_bytes(module)])


def claim_affiliate_rewards_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [CLAIM_AFFILIATE_REWARDS_FEATURE_DISABLED, _to_address_bytes(module)])


def claim_ui_fees_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [CLAIM_UI_FEES_FEATURE_DISABLED, _to_address_bytes(module)])


def subaccount_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [SUBACCOUNT_FEATURE_DISABLED, _to_address_bytes(module)])


def gasless_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [GASLESS_FEATURE_DISABLED, _to_address_bytes(module)])


def general_claim_feature_disabled(distribution_id: int) -> bytes:
    return _keccak_abi(["bytes32", "uint256"], [GENERAL_CLAIM_FEATURE_DISABLED, distribution_id])


def ui_fee_factor_key(account: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [UI_FEE_FACTOR, _to_address_bytes(account)])


def is_oracle_provider_enabled_key(provider: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [IS_ORACLE_PROVIDER_ENABLED, _to_address_bytes(provider)])


def is_atomic_oracle_provider_key(provider: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [IS_ATOMIC_ORACLE_PROVIDER, _to_address_bytes(provider)])


def oracle_timestamp_adjustment_key(provider: BytesLike, token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address", "address"], [ORACLE_TIMESTAMP_ADJUSTMENT, _to_address_bytes(provider), _to_address_bytes(token)])


def oracle_provider_for_token_key(oracle_or_token: BytesLike, token: BytesLike = None) -> bytes:
    # Solidity has two overloads: (oracle, token) and (token)
    if token is None:
        return _keccak_abi(["bytes32", "address"], [ORACLE_PROVIDER_FOR_TOKEN, _to_address_bytes(oracle_or_token)])
    return _keccak_abi(["bytes32", "address", "address"], [ORACLE_PROVIDER_FOR_TOKEN, _to_address_bytes(oracle_or_token), _to_address_bytes(token)])


def oracle_provider_updated_at_key(token: BytesLike, provider: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address", "address"], [ORACLE_PROVIDER_UPDATED_AT, _to_address_bytes(token), _to_address_bytes(provider)])


def token_transfer_gas_limit_key(token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [TOKEN_TRANSFER_GAS_LIMIT, _to_address_bytes(token)])


def saved_callback_contract(account: BytesLike, market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address", "address"], [SAVED_CALLBACK_CONTRACT, _to_address_bytes(account), _to_address_bytes(market)])


def min_collateral_factor_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [MIN_COLLATERAL_FACTOR, _to_address_bytes(market)])


def min_collateral_factor_for_open_interest_multiplier_key(market: BytesLike, is_long: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [MIN_COLLATERAL_FACTOR_FOR_OPEN_INTEREST_MULTIPLIER, _to_address_bytes(market), is_long])


def min_collateral_factor_for_liquidation_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [MIN_COLLATERAL_FACTOR_FOR_LIQUIDATION, _to_address_bytes(market)])


def virtual_token_id_key(token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [VIRTUAL_TOKEN_ID, _to_address_bytes(token)])


def virtual_market_id_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [VIRTUAL_MARKET_ID, _to_address_bytes(market)])


def virtual_inventory_for_positions_key(virtual_token_id: bytes) -> bytes:
    return _keccak_abi(["bytes32", "bytes32"], [VIRTUAL_INVENTORY_FOR_POSITIONS, virtual_token_id])


def virtual_inventory_for_positions_in_tokens_key(virtual_token_id: bytes) -> bytes:
    return _keccak_abi(["bytes32", "bytes32"], [VIRTUAL_INVENTORY_FOR_POSITIONS_IN_TOKENS, virtual_token_id])


def virtual_inventory_for_swaps_key(virtual_market_id: bytes, is_long_token: bool) -> bytes:
    return _keccak_abi(["bytes32", "bytes32", "bool"], [VIRTUAL_INVENTORY_FOR_SWAPS, virtual_market_id, is_long_token])


def position_impact_factor_key(market: BytesLike, is_positive: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [POSITION_IMPACT_FACTOR, _to_address_bytes(market), is_positive])


def position_impact_exponent_factor_key(market: BytesLike, is_positive: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [POSITION_IMPACT_EXPONENT_FACTOR, _to_address_bytes(market), is_positive])


def max_position_impact_factor_key(market: BytesLike, is_positive: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [MAX_POSITION_IMPACT_FACTOR, _to_address_bytes(market), is_positive])


def max_position_impact_factor_for_liquidations_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [MAX_POSITION_IMPACT_FACTOR_FOR_LIQUIDATIONS, _to_address_bytes(market)])


def position_fee_factor_key(market: BytesLike, balance_was_improved: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [POSITION_FEE_FACTOR, _to_address_bytes(market), balance_was_improved])


def pro_trader_tier_key(account: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [PRO_TRADER_TIER, _to_address_bytes(account)])


def pro_discount_factor_key(pro_tier: int) -> bytes:
    return _keccak_abi(["bytes32", "uint256"], [PRO_DISCOUNT_FACTOR, pro_tier])


def liquidation_fee_factor_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [LIQUIDATION_FEE_FACTOR, _to_address_bytes(market)])


def swap_impact_factor_key(market: BytesLike, is_positive: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [SWAP_IMPACT_FACTOR, _to_address_bytes(market), is_positive])


def swap_impact_exponent_factor_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [SWAP_IMPACT_EXPONENT_FACTOR, _to_address_bytes(market)])


def swap_fee_factor_key(market: BytesLike, balance_was_improved: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [SWAP_FEE_FACTOR, _to_address_bytes(market), balance_was_improved])


def atomic_swap_fee_factor_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [ATOMIC_SWAP_FEE_FACTOR, _to_address_bytes(market)])


def atomic_withdrawal_fee_factor_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [ATOMIC_WITHDRAWAL_FEE_FACTOR, _to_address_bytes(market)])


def deposit_fee_factor_key(market: BytesLike, balance_was_improved: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [DEPOSIT_FEE_FACTOR, _to_address_bytes(market), balance_was_improved])


def withdrawal_fee_factor_key(market: BytesLike, balance_was_improved: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [WITHDRAWAL_FEE_FACTOR, _to_address_bytes(market), balance_was_improved])


def oracle_type_key(token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [ORACLE_TYPE, _to_address_bytes(token)])


def open_interest_key(market: BytesLike, collateral_token: BytesLike, is_long: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "address", "bool"], [OPEN_INTEREST, _to_address_bytes(market), _to_address_bytes(collateral_token), is_long])


def open_interest_in_tokens_key(market: BytesLike, collateral_token: BytesLike, is_long: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "address", "bool"], [OPEN_INTEREST_IN_TOKENS, _to_address_bytes(market), _to_address_bytes(collateral_token), is_long])


def collateral_sum_key(market: BytesLike, collateral_token: BytesLike, is_long: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "address", "bool"], [COLLATERAL_SUM, _to_address_bytes(market), _to_address_bytes(collateral_token), is_long])


def max_collateral_sum_key(market: BytesLike, collateral_token: BytesLike, is_long: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "address", "bool"], [MAX_COLLATERAL_SUM, _to_address_bytes(market), _to_address_bytes(collateral_token), is_long])


def pool_amount_key(market: BytesLike, token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address", "address"], [POOL_AMOUNT, _to_address_bytes(market), _to_address_bytes(token)])


def max_pool_amount_key(market: BytesLike, token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address", "address"], [MAX_POOL_AMOUNT, _to_address_bytes(market), _to_address_bytes(token)])


def max_pool_usd_for_deposit_key(market: BytesLike, token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address", "address"], [MAX_POOL_USD_FOR_DEPOSIT, _to_address_bytes(market), _to_address_bytes(token)])


def max_open_interest_key(market: BytesLike, is_long: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [MAX_OPEN_INTEREST, _to_address_bytes(market), is_long])


def position_impact_pool_amount_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [POSITION_IMPACT_POOL_AMOUNT, _to_address_bytes(market)])


def lent_position_impact_pool_amount_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [LENT_POSITION_IMPACT_POOL_AMOUNT, _to_address_bytes(market)])


def max_lendable_impact_factor_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [MAX_LENDABLE_IMPACT_FACTOR, _to_address_bytes(market)])


def max_lendable_impact_factor_for_withdrawals_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [MAX_LENDABLE_IMPACT_FACTOR_FOR_WITHDRAWALS, _to_address_bytes(market)])


def max_lendable_impact_usd_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [MAX_LENDABLE_IMPACT_USD, _to_address_bytes(market)])


def total_pending_impact_amount_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [TOTAL_PENDING_IMPACT_AMOUNT, _to_address_bytes(market)])


def min_position_impact_pool_amount_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [MIN_POSITION_IMPACT_POOL_AMOUNT, _to_address_bytes(market)])


def position_impact_pool_distribution_rate_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [POSITION_IMPACT_POOL_DISTRIBUTION_RATE, _to_address_bytes(market)])


def position_impact_pool_distributed_at_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [POSITION_IMPACT_POOL_DISTRIBUTED_AT, _to_address_bytes(market)])


def swap_impact_pool_amount_key(market: BytesLike, token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address", "address"], [SWAP_IMPACT_POOL_AMOUNT, _to_address_bytes(market), _to_address_bytes(token)])


def reserve_factor_key(market: BytesLike, is_long: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [RESERVE_FACTOR, _to_address_bytes(market), is_long])


def open_interest_reserve_factor_key(market: BytesLike, is_long: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [OPEN_INTEREST_RESERVE_FACTOR, _to_address_bytes(market), is_long])


def max_pnl_factor_key(pnl_factor_type: bytes, market: BytesLike, is_long: bool) -> bytes:
    return _keccak_abi(["bytes32", "bytes32", "address", "bool"], [MAX_PNL_FACTOR, pnl_factor_type, _to_address_bytes(market), is_long])


def min_pnl_factor_after_adl_key(market: BytesLike, is_long: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [MIN_PNL_FACTOR_AFTER_ADL, _to_address_bytes(market), is_long])


def latest_adl_at_key(market: BytesLike, is_long: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [LATEST_ADL_AT, _to_address_bytes(market), is_long])


def is_adl_enabled_key(market: BytesLike, is_long: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [IS_ADL_ENABLED, _to_address_bytes(market), is_long])


def funding_factor_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [FUNDING_FACTOR, _to_address_bytes(market)])


def funding_exponent_factor_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [FUNDING_EXPONENT_FACTOR, _to_address_bytes(market)])


def saved_funding_factor_per_second_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [SAVED_FUNDING_FACTOR_PER_SECOND, _to_address_bytes(market)])


def funding_increase_factor_per_second_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [FUNDING_INCREASE_FACTOR_PER_SECOND, _to_address_bytes(market)])


def funding_decrease_factor_per_second_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [FUNDING_DECREASE_FACTOR_PER_SECOND, _to_address_bytes(market)])


def min_funding_factor_per_second_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [MIN_FUNDING_FACTOR_PER_SECOND, _to_address_bytes(market)])


def max_funding_factor_per_second_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [MAX_FUNDING_FACTOR_PER_SECOND, _to_address_bytes(market)])


def threshold_for_stable_funding_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [THRESHOLD_FOR_STABLE_FUNDING, _to_address_bytes(market)])


def threshold_for_decrease_funding_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [THRESHOLD_FOR_DECREASE_FUNDING, _to_address_bytes(market)])


def funding_fee_amount_per_size_key(market: BytesLike, collateral_token: BytesLike, is_long: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "address", "bool"], [FUNDING_FEE_AMOUNT_PER_SIZE, _to_address_bytes(market), _to_address_bytes(collateral_token), is_long])


def claimable_funding_amount_per_size_key(market: BytesLike, collateral_token: BytesLike, is_long: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "address", "bool"], [CLAIMABLE_FUNDING_AMOUNT_PER_SIZE, _to_address_bytes(market), _to_address_bytes(collateral_token), is_long])


def funding_updated_at_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [FUNDING_UPDATED_AT, _to_address_bytes(market)])


def claimable_funding_amount_key(market: BytesLike, token: BytesLike, account: BytesLike = None) -> bytes:
    if account is None:
        return _keccak_abi(["bytes32", "address", "address"], [CLAIMABLE_FUNDING_AMOUNT, _to_address_bytes(market), _to_address_bytes(token)])
    return _keccak_abi(["bytes32", "address", "address", "address"], [CLAIMABLE_FUNDING_AMOUNT, _to_address_bytes(market), _to_address_bytes(token), _to_address_bytes(account)])


def claimable_collateral_amount_key(market: BytesLike, token: BytesLike, time_key: int = None, account: BytesLike = None) -> bytes:
    if time_key is None and account is None:
        return _keccak_abi(["bytes32", "address", "address"], [CLAIMABLE_COLLATERAL_AMOUNT, _to_address_bytes(market), _to_address_bytes(token)])
    return _keccak_abi(["bytes32", "address", "address", "uint256", "address"], [CLAIMABLE_COLLATERAL_AMOUNT, _to_address_bytes(market), _to_address_bytes(token), time_key, _to_address_bytes(account)])


def claimable_collateral_factor_key(market: BytesLike, token: BytesLike, time_key: int, account: BytesLike = None) -> bytes:
    if account is None:
        return _keccak_abi(["bytes32", "address", "address", "uint256"], [CLAIMABLE_COLLATERAL_FACTOR, _to_address_bytes(market), _to_address_bytes(token), time_key])
    return _keccak_abi(["bytes32", "address", "address", "uint256", "address"], [CLAIMABLE_COLLATERAL_FACTOR, _to_address_bytes(market), _to_address_bytes(token), time_key, _to_address_bytes(account)])


def claimable_collateral_reduction_factor_key(market: BytesLike, token: BytesLike, time_key: int, account: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address", "address", "uint256", "address"], [CLAIMABLE_COLLATERAL_REDUCTION_FACTOR, _to_address_bytes(market), _to_address_bytes(token), time_key, _to_address_bytes(account)])


def claimed_collateral_amount_key(market: BytesLike, token: BytesLike, time_key: int, account: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address", "address", "uint256", "address"], [CLAIMED_COLLATERAL_AMOUNT, _to_address_bytes(market), _to_address_bytes(token), time_key, _to_address_bytes(account)])


def optimal_usage_factor_key(market: BytesLike, is_long: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [OPTIMAL_USAGE_FACTOR, _to_address_bytes(market), is_long])


def base_borrowing_factor_key(market: BytesLike, is_long: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [BASE_BORROWING_FACTOR, _to_address_bytes(market), is_long])


def above_optimal_usage_borrowing_factor_key(market: BytesLike, is_long: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [ABOVE_OPTIMAL_USAGE_BORROWING_FACTOR, _to_address_bytes(market), is_long])


def borrowing_factor_key(market: BytesLike, is_long: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [BORROWING_FACTOR, _to_address_bytes(market), is_long])


def borrowing_exponent_factor_key(market: BytesLike, is_long: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [BORROWING_EXPONENT_FACTOR, _to_address_bytes(market), is_long])


def cumulative_borrowing_factor_key(market: BytesLike, is_long: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [CUMULATIVE_BORROWING_FACTOR, _to_address_bytes(market), is_long])


def cumulative_borrowing_factor_updated_at_key(market: BytesLike, is_long: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [CUMULATIVE_BORROWING_FACTOR_UPDATED_AT, _to_address_bytes(market), is_long])


def total_borrowing_key(market: BytesLike, is_long: bool) -> bytes:
    return _keccak_abi(["bytes32", "address", "bool"], [TOTAL_BORROWING, _to_address_bytes(market), is_long])


def affiliate_reward_key(market: BytesLike, token: BytesLike, account: BytesLike = None) -> bytes:
    if account is None:
        return _keccak_abi(["bytes32", "address", "address"], [AFFILIATE_REWARD, _to_address_bytes(market), _to_address_bytes(token)])
    return _keccak_abi(["bytes32", "address", "address", "address"], [AFFILIATE_REWARD, _to_address_bytes(market), _to_address_bytes(token), _to_address_bytes(account)])


def min_affiliate_reward_factor_key(referral_tier_level: int) -> bytes:
    return _keccak_abi(["bytes32", "uint256"], [MIN_AFFILIATE_REWARD_FACTOR, referral_tier_level])


def max_allowed_subaccount_action_count_key(account: BytesLike, subaccount: BytesLike, action_type: bytes) -> bytes:
    return _keccak_abi(["bytes32", "address", "address", "bytes32"], [MAX_ALLOWED_SUBACCOUNT_ACTION_COUNT, _to_address_bytes(account), _to_address_bytes(subaccount), action_type])


def subaccount_expires_at_key(account: BytesLike, subaccount: BytesLike, action_type: bytes) -> bytes:
    return _keccak_abi(["bytes32", "address", "address", "bytes32"], [SUBACCOUNT_EXPIRES_AT, _to_address_bytes(account), _to_address_bytes(subaccount), action_type])


def subaccount_action_count_key(account: BytesLike, subaccount: BytesLike, action_type: bytes) -> bytes:
    return _keccak_abi(["bytes32", "address", "address", "bytes32"], [SUBACCOUNT_ACTION_COUNT, _to_address_bytes(account), _to_address_bytes(subaccount), action_type])


def subaccount_auto_top_up_amount_key(account: BytesLike, subaccount: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address", "address"], [SUBACCOUNT_AUTO_TOP_UP_AMOUNT, _to_address_bytes(account), _to_address_bytes(subaccount)])


def subaccount_integration_id_key(account: BytesLike, subaccount: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address", "address"], [SUBACCOUNT_INTEGRATION_ID, _to_address_bytes(account), _to_address_bytes(subaccount)])


def subaccount_integration_disabled_key(integration_id: bytes) -> bytes:
    return _keccak_abi(["bytes32", "bytes32"], [SUBACCOUNT_INTEGRATION_DISABLED, integration_id])


def is_market_disabled_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [IS_MARKET_DISABLED, _to_address_bytes(market)])


def min_market_tokens_for_first_deposit_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [MIN_MARKET_TOKENS_FOR_FIRST_DEPOSIT, _to_address_bytes(market)])


def price_feed_key(token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [PRICE_FEED, _to_address_bytes(token)])


def data_stream_id_key(token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [DATA_STREAM_ID, _to_address_bytes(token)])


def edge_data_stream_id_key(token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [EDGE_DATA_STREAM_ID, _to_address_bytes(token)])


def edge_data_stream_token_decimals_key(token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [EDGE_DATA_STREAM_TOKEN_DECIMALS, _to_address_bytes(token)])


def data_stream_multiplier_key(token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [DATA_STREAM_MULTIPLIER, _to_address_bytes(token)])


def data_stream_spread_reduction_factor_key(token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [DATA_STREAM_SPREAD_REDUCTION_FACTOR, _to_address_bytes(token)])


def price_feed_multiplier_key(token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [PRICE_FEED_MULTIPLIER, _to_address_bytes(token)])


def price_feed_heartbeat_duration_key(token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [PRICE_FEED_HEARTBEAT_DURATION, _to_address_bytes(token)])


def stable_price_key(token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [STABLE_PRICE, _to_address_bytes(token)])


def fee_distributor_swap_token_index_key(order_key: bytes) -> bytes:
    return _keccak_abi(["bytes32", "bytes32"], [FEE_DISTRIBUTOR_SWAP_TOKEN_INDEX, order_key])


def fee_distributor_swap_fee_batch_key(order_key: bytes) -> bytes:
    return _keccak_abi(["bytes32", "bytes32"], [FEE_DISTRIBUTOR_SWAP_FEE_BATCH, order_key])


def glv_max_market_token_balance_usd_key(glv: BytesLike, market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address", "address"], [GLV_MAX_MARKET_TOKEN_BALANCE_USD, _to_address_bytes(glv), _to_address_bytes(market)])


def glv_max_market_token_balance_amount_key(glv: BytesLike, market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address", "address"], [GLV_MAX_MARKET_TOKEN_BALANCE_AMOUNT, _to_address_bytes(glv), _to_address_bytes(market)])


def is_glv_market_disabled_key(glv: BytesLike, market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address", "address"], [IS_GLV_MARKET_DISABLED, _to_address_bytes(glv), _to_address_bytes(market)])


def glv_shift_max_loss_factor_key(glv: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [GLV_SHIFT_MAX_LOSS_FACTOR, _to_address_bytes(glv)])


def glv_shift_last_executed_at_key(glv: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [GLV_SHIFT_LAST_EXECUTED_AT, _to_address_bytes(glv)])


def glv_shift_min_interval_key(glv: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [GLV_SHIFT_MIN_INTERVAL, _to_address_bytes(glv)])


def min_glv_tokens_for_first_glv_deposit_key(glv: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [MIN_GLV_TOKENS_FOR_FIRST_DEPOSIT, _to_address_bytes(glv)])


def sync_config_feature_disabled_key(module: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [SYNC_CONFIG_FEATURE_DISABLED, _to_address_bytes(module)])


def sync_config_market_disabled_key(market: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [SYNC_CONFIG_MARKET_DISABLED, _to_address_bytes(market)])


def sync_config_parameter_disabled_key(parameter: str) -> bytes:
    return _keccak_abi(["bytes32", "string"], [SYNC_CONFIG_PARAMETER_DISABLED, parameter])


def sync_config_market_parameter_disabled_key(market: BytesLike, parameter: str) -> bytes:
    return _keccak_abi(["bytes32", "address", "string"], [SYNC_CONFIG_MARKET_PARAMETER_DISABLED, _to_address_bytes(market), parameter])


def sync_config_update_completed_key(update_id: int) -> bytes:
    return _keccak_abi(["bytes32", "uint256"], [SYNC_CONFIG_UPDATE_COMPLETED, update_id])


def sync_config_latest_update_id_key() -> bytes:
    return SYNC_CONFIG_LATEST_UPDATE_ID


def contributor_token_amount_key(account: BytesLike, token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address", "address"], [CONTRIBUTOR_TOKEN_AMOUNT, _to_address_bytes(account), _to_address_bytes(token)])


def max_total_contributor_token_amount_key(token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [MAX_TOTAL_CONTRIBUTOR_TOKEN_AMOUNT, _to_address_bytes(token)])


def contributor_funding_account_key(token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [CONTRIBUTOR_FUNDING_ACCOUNT, _to_address_bytes(token)])


def custom_contributor_funding_account_key(account: BytesLike, token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address", "address"], [CUSTOM_CONTRIBUTOR_FUNDING_ACCOUNT, _to_address_bytes(account), _to_address_bytes(token)])


def buyback_batch_amount_key(token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [BUYBACK_BATCH_AMOUNT, _to_address_bytes(token)])


def buyback_available_fee_amount_key(fee_token: BytesLike, swap_token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address", "address"], [BUYBACK_AVAILABLE_FEE_AMOUNT, _to_address_bytes(fee_token), _to_address_bytes(swap_token)])


def withdrawable_buyback_token_amount_key(buyback_token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [WITHDRAWABLE_BUYBACK_TOKEN_AMOUNT, _to_address_bytes(buyback_token)])


def buyback_gmx_factor_key(version: int) -> bytes:
    return _keccak_abi(["bytes32", "uint256"], [BUYBACK_GMX_FACTOR, version])


def buyback_max_price_impact_factor_key(token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [BUYBACK_MAX_PRICE_IMPACT_FACTOR, _to_address_bytes(token)])


def is_multichain_provider_enabled_key(provider: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [IS_MULTICHAIN_PROVIDER_ENABLED, _to_address_bytes(provider)])


def is_multichain_endpoint_enabled_key(endpoint: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [IS_MULTICHAIN_ENDPOINT_ENABLED, _to_address_bytes(endpoint)])


def is_relay_fee_excluded_key(sender: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [IS_RELAY_FEE_EXCLUDED, _to_address_bytes(sender)])


def is_src_chain_id_enabled_key(src_chain_id: int) -> bytes:
    return _keccak_abi(["bytes32", "uint256"], [IS_SRC_CHAIN_ID_ENABLED, src_chain_id])


def position_last_src_chain_id(position_key: bytes) -> bytes:
    return _keccak_abi(["bytes32", "bytes32"], [POSITION_LAST_SRC_CHAIN_ID, position_key])


def eid_to_src_chain_id(eid: int) -> bytes:
    return _keccak_abi(["bytes32", "uint32"], [EID_TO_SRC_CHAIN_ID, eid])


def multichain_balance_key(account: BytesLike, token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address", "address"], [MULTICHAIN_BALANCE, _to_address_bytes(account), _to_address_bytes(token)])


def claimable_funds_amount_key(account: BytesLike, token: BytesLike, distribution_id: int) -> bytes:
    return _keccak_abi(["bytes32", "address", "address", "uint256"], [CLAIMABLE_FUNDS_AMOUNT, _to_address_bytes(account), _to_address_bytes(token), distribution_id])


def total_claimable_funds_amount_key(token: BytesLike) -> bytes:
    return _keccak_abi(["bytes32", "address"], [TOTAL_CLAIMABLE_FUNDS_AMOUNT, _to_address_bytes(token)])


def claim_terms_key(distribution_id: int) -> bytes:
    return _keccak_abi(["bytes32", "uint256"], [CLAIM_TERMS, distribution_id])


def claim_terms_backref_key(terms_hash: bytes) -> bytes:
    return _keccak_abi(["bytes32", "bytes32"], [CLAIM_TERMS_BACKREF, terms_hash])


__all__ = [name for name in globals() if name.isupper()]
