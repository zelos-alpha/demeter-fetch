import os

import pandas as pd
from tqdm import tqdm

import processor_tick
import processor_minute
import source_big_query
import source_rpc
import source_file
from _typing import *
from utils import print_log


def download(config: Config):
    raw_file_list = []
    match config.from_config.data_source:
        case DataSource.rpc:
            raw_file_list = source_rpc.download_event(chain=config.from_config.chain,
                                                      pool_addr=config.from_config.pool_address,
                                                      end_point=config.from_config.rpc.end_point,
                                                      start=config.from_config.rpc.start,
                                                      end=config.from_config.rpc.end,
                                                      batch_size=config.from_config.rpc.batch_size,
                                                      auth_string=config.from_config.rpc.auth_string,
                                                      http_proxy=config.from_config.rpc.http_proxy)
            proxy_path = config.from_config.rpc.proxy_file_path
            # if need proxy:
            #     merge
        case DataSource.big_query:
            raw_file_list = source_big_query.download_event(config.from_config.chain,
                                                            config.from_config.pool_address,
                                                            config.from_config.big_query.start,
                                                            config.from_config.big_query.end,
                                                            config.to_config.save_path,
                                                            config.from_config.big_query.auth_file,
                                                            config.from_config.big_query.http_proxy)
        case DataSource.file:
            raw_file_list, proxy_path = source_file.load_raw_files(config.from_config.file)
            # if need proxy:
            #     merge
    print("\n")
    print_log(f"Download finish")
    if config.to_config.type != ToType.raw:
        print_log(f"Start generate {len(raw_file_list)} files")
        generate_to_files(config.to_config, raw_file_list)


def generate_to_files(to_config: ToConfig, raw_files: List[str]):
    with tqdm(total=len(raw_files), ncols=150) as pbar:
        for file in raw_files:
            df = pd.read_csv(file)
            # df = df.rename(columns={
            #     "log_index": "pool_log_index",
            #     "transaction_index": "pool_tx_index",
            #     "DATA": "pool_data",
            #     "topics": "pool_topics",
            # })
            result_df = pd.DataFrame()
            df = df.sort_values(['block_number', 'pool_log_index'], ascending=[True, True], ignore_index=True)

            match to_config.type:
                case ToType.minute:
                    result_df = processor_minute.preprocess_one(df)
                case ToType.tick:
                    result_df = processor_tick.preprocess_one(df)
            file_name = os.path.basename(file)
            file_name_and_ext = os.path.splitext(file_name)
            result_df.to_csv(os.path.join(to_config.save_path, f"{file_name_and_ext[0].replace('.raw', '')}.{to_config.type.name}{file_name_and_ext[1]}"), index=False)
            pbar.update()
