import os
from decimal import Decimal
from typing import Tuple
import pandas as pd
from tqdm import tqdm
from demeter_fetch._typing import ChainType, ChainTypeConfig


def to_decimal(value) -> Decimal:
    return Decimal(value) if value else Decimal(0)


def to_int(value) -> int:
    return int(Decimal(value)) if value else 0


def get_tick_key(tx_row: pd.Series) -> Tuple[int, int]:
    return int(tx_row["tick_lower"]), int(tx_row["tick_upper"])


def get_pos_key(tx: pd.Series) -> str:
    if tx["position_id"] and not pd.isna(tx["position_id"]):
        return str(int(tx["position_id"]))
    return f'{tx["sender"]}-{int(tx["tick_lower"])}-{int(tx["tick_upper"])}'


def generate_user_position(config):
    position_history = []
    position_address_dict = {}
    transfers = pd.DataFrame()


    dt_lst = pd.date_range(config.from_config.big_query.start,
                          config.from_config.big_query.end).strftime("%Y-%m-%d").to_list()
    for dt in tqdm(dt_lst, desc='Loading proxy logs.'):
        file = f'{config.to_config.save_path}/{config.from_config.chain}-{ChainTypeConfig[config.from_config.chain]["uniswap_proxy_addr"]}-{dt}.raw.csv'
        df = pd.read_csv(file, dtype=str)
        transfers = pd.concat([transfers, df])
        transfers['position_id'] = transfers['position_id'].apply(int)
        transfers['block_number'] = transfers['block_number'].apply(int)

    for dt in tqdm(dt_lst, desc='Loading position data.'):
        file = f'{config.to_config.save_path}/{config.from_config.chain}-{config.from_config.uniswap_config.pool_address}-{dt}.{config.to_config.type}.csv'
        df = pd.read_csv(file)
        df_mint_burn = df[df['tx_type'].isin(['MINT', 'BURN'])]
        df_filter = df_mint_burn[~ pd.isnull(df_mint_burn["position_id"])]
        df_filter = df_filter[["block_number", 'block_timestamp', 'position_id', 'liquidity', 'tick_lower',
                               'tick_upper', 'sender', 'receipt']]
        to_del_index = []
        for index, tx in df_filter.iterrows():
            rel_transfer = transfers[(transfers["position_id"] == int(tx["position_id"])) &
                                     (transfers["block_number"] <= tx["block_number"])]
            if len(rel_transfer.index) == 0:
                to_del_index.append(index)
                continue
            rel_transfer = rel_transfer.tail(1)
            df_mint_burn.loc[index, "receipt"] = df_filter.loc[index, "sender"]
            df_mint_burn.loc[index, "sender"] = rel_transfer.iloc[0]["to"]
        df_mint_burn = df_mint_burn.drop(to_del_index)
        try:
            df_mint_burn['amount0'] = df_mint_burn['amount0'].apply(lambda x: to_decimal(x))
            df_mint_burn['amount1'] = df_mint_burn['amount1'].apply(lambda x: to_decimal(x))
            df_mint_burn['liquidity'] = df_mint_burn['liquidity'].apply(lambda x: to_int(x))
        except Exception as ex:
            print(ex)
        df_mint_burn["block_timestamp"] = pd.to_datetime(df_mint_burn["block_timestamp"], format="%Y-%m-%d %H:%M:%S")

        for tx_index, tx in df_mint_burn.iterrows():
            pos_key_str = get_pos_key(tx)
            tick_key = get_tick_key(tx)
            amount0 = amount1 = liquidity = Decimal(0)
            match tx["tx_type"]:
                case "MINT":
                    liquidity = tx["liquidity"]
                    amount0 = tx["amount0"]
                    amount1 = tx["amount1"]
                case "BURN":
                    liquidity = Decimal(0) - tx["liquidity"]
                    amount0 = tx["amount0"]
                    amount1 = tx["amount1"]
            position_history.append({
                'id': pos_key_str,
                'lower_tick': tick_key[0],
                'upper_tick': tick_key[1],
                'tx_type': tx["tx_type"],
                'block_number': tx["block_number"],
                'tx_hash': tx["transaction_hash"],
                'log_index': tx["pool_log_index"],
                'blk_time': tx["block_timestamp"],
                'liquidity': liquidity,
                'final_amount0': amount0,
                'final_amount1': amount1,
            })
        df_mint = df_mint_burn[df_mint_burn["tx_type"] == "MINT"]
        for index, row in df_mint.iterrows():
            pos_key = get_pos_key(row)
            if pos_key not in position_address_dict:
                position_address_dict[pos_key] = {"address": row["sender"]}

    df_position_liquidity = pd.DataFrame(position_history)
    df_position_liquidity.sort_values(["id", "block_number", "log_index"], inplace=True)
    pos_data = []
    for pos, data in tqdm(position_address_dict.items(), desc='Generate position address rel.'):
        pos_data.append({"position": pos, "address": data["address"]})
    df_position_address = pd.DataFrame(pos_data)

    df_pos = df_position_liquidity[df_position_liquidity['tx_type'].isin(['MINT', 'BURN'])]
    result = []
    for pos_id, pos_group in tqdm(df_pos.groupby('id'), desc='Generate liquidity record.'):
        pos_group['liquidity'] = pos_group['liquidity'].apply(Decimal)
        final_result = []
        group_result = []
        for index, row in pos_group.iterrows():
            if row['tx_type'] == 'MINT':
                if not group_result:
                    group_result.append({
                        'position': pos_id,
                        'tx_type': 'MINT',
                        'mint_dt': row['blk_time'],
                        'liquidity': row['liquidity'],
                        'lower_tick': row['lower_tick'],
                        'upper_tick': row['upper_tick'],
                    })
                else:
                    exist = group_result.pop()
                    final_result.append({
                        'position': pos_id,
                        'mint_dt': exist['mint_dt'],
                        'burn_dt': row['blk_time'],
                        'liquidity': exist['liquidity'],
                        'lower_tick': row['lower_tick'],
                        'upper_tick': row['upper_tick'],
                    })
                    group_result.append({
                        'position': pos_id,
                        'tx_type': 'MINT',
                        'mint_dt': row['blk_time'],
                        'liquidity': row['liquidity'] + exist['liquidity'],
                        'lower_tick': row['lower_tick'],
                        'upper_tick': row['upper_tick'],
                    })
            if row['tx_type'] == 'BURN':
                if not group_result:
                    pass
                else:
                    exist = group_result.pop()
                    burn_liq = row['liquidity']
                    if burn_liq == Decimal('0'):
                        continue
                    final_result.append({
                        'position': pos_id,
                        'mint_dt': exist['mint_dt'],
                        'burn_dt': row['blk_time'],
                        'liquidity': exist['liquidity'],
                        'lower_tick': row['lower_tick'],
                        'upper_tick': row['upper_tick'],
                    })
                    if abs(burn_liq) < exist['liquidity']:
                        group_result.append({
                            'position': pos_id,
                            'tx_type': 'MINT',
                            'mint_dt': row['blk_time'],
                            'liquidity': exist['liquidity'] + burn_liq,
                            'lower_tick': row['lower_tick'],
                            'upper_tick': row['upper_tick'],
                        })
        if group_result:
            last_item = group_result.pop()
            last_item['burn_dt'] = None
            last_item.pop('tx_type', None)
            final_result.append(last_item)
        result.extend(final_result)

    df_position_record = pd.DataFrame(result)
    df_merge = pd.merge(df_position_record, df_position_address, how='left', left_on='position', right_on='position')
    df_merge = df_merge[['address', 'position', 'mint_dt', 'burn_dt', 'liquidity', 'lower_tick', 'upper_tick']]
    df_merge.to_csv(os.path.join(config.to_config.save_path, 'position_address_record.csv'), index=False)
