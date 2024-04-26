from collections import namedtuple
from decimal import Decimal
from typing import Dict, Callable, List

import pandas as pd
from tqdm import tqdm

from .. import ChainType
from ..common import (
    ChainTypeConfig,
    NodeNames,
    Node,
    set_global_pbar,
    EmptyNamedTuple,
    to_decimal,
    get_depend_name,
)
from ..sources.rpc_utils import set_position_id


class UniUserLP(Node):
    name = NodeNames.uni_user_lp

    def _get_file_name(self, param: namedtuple) -> str:
        return (
            f"{self.from_config.chain.name}-{self.from_config.uniswap_config.pool_address}-"
            f"{self.from_config.start.strftime('%Y-%m-%d')}-{self.from_config.end.strftime('%Y-%m-%d')}.user_lp"
            + self._get_file_ext()
        )

    @property
    def _load_csv_converter(self) -> Dict[str, Callable]:
        return {
            "liquidity": to_decimal,
        }

    @property
    def _parse_date_column(self) -> List[str]:
        return ["start_time", "end_time"]

    def _process_one(self, data: Dict[str, List[str]], param: EmptyNamedTuple) -> pd.DataFrame():
        position_df = self.get_depend_by_name(NodeNames.uni_positions, self.id).read_file(
            data[get_depend_name(NodeNames.uni_positions, self.id)][0]
        )
        position_df = position_df[position_df["tx_type"].isin(["MINT", "BURN"])]
        position_df["liq_delta"] = position_df.apply(
            lambda r: -r["liquidity"] if r["tx_type"] == "BURN" else r["liquidity"], axis=1
        )
        grouped_position = position_df.groupby(["position_id"])
        user_lp_list = []
        for idx, position_tx in grouped_position:
            liquidity = Decimal(0)
            # before start time, if there are a mint, last liquidity might be negative
            # e.g. mint 10, before start time, then mint 20, and burn 30, We will only find mint 20 and burn 30,
            # liquidity at last will be -10
            # In this situation, we insert a moke mint transaction to add 10 liquidity.
            total_liq = position_tx["liq_delta"].sum()
            if total_liq < 0:
                user_lp_list.append(
                    {
                        "address": position_tx.iloc[0]["owner"],
                        "position_id": idx[0],
                        "tick_lower": position_tx.iloc[0]["tick_lower"],
                        "tick_upper": position_tx.iloc[0]["tick_upper"],
                        "start_time": None,
                        "end_time": position_tx.iloc[0]["block_timestamp"],
                        "liquidity": -total_liq,  # ,
                        "start_hash": "",
                        "end_hash": position_tx.iloc[0]["transaction_hash"],
                        "log_index": 0,
                        "block_number": position_tx.iloc[0]["block_number"] - 1,
                    }
                )
                liquidity = total_liq
            for i in range(len(position_tx.index)):
                if position_tx.iloc[i]["tx_type"] == "MINT":
                    liquidity += position_tx.iloc[i]["liquidity"]
                elif position_tx.iloc[i]["tx_type"] == "BURN":
                    liquidity -= position_tx.iloc[i]["liquidity"]
                    if liquidity <= 0:
                        continue
                row = {
                    "address": position_tx.iloc[i]["owner"],
                    "position_id": idx[0],
                    "tick_lower": position_tx.iloc[i]["tick_lower"],
                    "tick_upper": position_tx.iloc[i]["tick_upper"],
                    "start_time": position_tx.iloc[i]["block_timestamp"],
                    "end_time": None,
                    "liquidity": liquidity,  # ,
                    "start_hash": position_tx.iloc[i]["transaction_hash"],
                    "end_hash": "",
                    "log_index": position_tx.iloc[i]["pool_log_index"],
                    "block_number": position_tx.iloc[i]["block_number"],
                }
                if i < len(position_tx.index) - 1:
                    row["end_time"] = position_tx.iloc[i + 1]["block_timestamp"]
                    row["end_hash"] = position_tx.iloc[i + 1]["transaction_hash"]
                user_lp_list.append(row)
        df = pd.DataFrame(user_lp_list)
        df = df.sort_values(["address", "position_id", "block_number", "log_index"])
        df = df.drop(columns=["block_number", "log_index"])
        return df


class UniPositions(Node):
    name = NodeNames.uni_positions

    def _get_file_name(self, param: namedtuple) -> str:
        return (
            f"{self.from_config.chain.name}-{self.from_config.uniswap_config.pool_address}-"
            f"{self.from_config.start.strftime('%Y-%m-%d')}-{self.from_config.end.strftime('%Y-%m-%d')}"
            f".positions" + self._get_file_ext()
        )

    @property
    def _load_csv_converter(self) -> Dict[str, Callable]:
        return {
            "amount0": to_decimal,
            "amount1": to_decimal,
            "liquidity": to_decimal,
            "sqrtPriceX96": to_decimal,
        }

    def get_tx_user(self, chain: ChainType, txes: pd.DataFrame) -> str:
        """
        Get real owner of this tx by trace token flow
        """
        if len(txes.index) < 1:
            return ""
        tx: pd.Series = txes.iloc[0]

        if tx["to"] == ChainTypeConfig[chain]["uniswap_proxy_addr"]:
            return tx["from"]
        else:
            return tx["to"]

    @property
    def _parse_date_column(self) -> List[str]:
        return ["block_timestamp"]

    def _process_one(self, data: Dict[str, List[str]], param: EmptyNamedTuple) -> pd.DataFrame:
        tick_csv_paths = data[get_depend_name(NodeNames.uni_tick, self.id)]
        log_csv_paths = data[get_depend_name(NodeNames.uni_tx, self.id)]
        tick_csv_paths.sort()
        log_csv_paths.sort()
        pbar = tqdm(total=(self.from_config.end - self.from_config.start).days + 1, ncols=80, position=0, leave=False)
        set_global_pbar(pbar)
        total_df = pd.DataFrame()
        for i in range(len(tick_csv_paths)):
            daily_tick_df = self.get_depend_by_name(NodeNames.uni_tick).read_file(tick_csv_paths[i])
            daily_tx_df = self.get_depend_by_name(NodeNames.uni_tx).read_file(log_csv_paths[i])
            daily_tick_df = daily_tick_df[daily_tick_df["tx_type"].isin(["MINT", "BURN", "COLLECT"])]
            tx_hashes = daily_tick_df["transaction_hash"].drop_duplicates()
            owners = {
                hash: self.get_tx_user(self.from_config.chain, daily_tx_df[daily_tx_df["transaction_hash"] == hash])
                for hash in tx_hashes
            }
            daily_tick_df["owner"] = daily_tick_df["transaction_hash"].apply(lambda hash: owners[hash])
            total_df = pd.concat([total_df, daily_tick_df])
            pbar.update()

        # set position id for empty
        total_df["position_id"] = total_df.apply(set_position_id, axis=1)
        total_df = total_df[
            [
                "position_id",
                "tx_type",
                "owner",
                "tick_lower",
                "tick_upper",
                "liquidity",
                "block_number",
                "block_timestamp",
                "transaction_hash",
                "pool_tx_index",
                "pool_log_index",
                "proxy_log_index",
                "sender",
                "receipt",
                "amount0",
                "amount1",
                "sqrtPriceX96",
                "current_tick",
            ]
        ]
        total_df = total_df.sort_values(["position_id", "block_number", "pool_log_index"])
        return total_df
