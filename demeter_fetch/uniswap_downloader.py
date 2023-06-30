import os

import pandas as pd
from tqdm import tqdm

import demeter_fetch.processor_minute as processor_minute
import demeter_fetch.processor_tick as processor_tick
import demeter_fetch.source_big_query as source_big_query
import demeter_fetch.source_file as source_file
import demeter_fetch.source_rpc as source_rpc
import demeter_fetch.source_chifra_log as source_chifra_log
from ._typing import *
from .utils import print_log
from multiprocessing import Pool


def download(config: Config):
    raw_file_list = []
    match config.from_config.data_source:
        case DataSource.rpc:
            raw_file_list, start, end = source_rpc.query_uniswap_pool_logs(chain=config.from_config.chain,
                                                                           pool_addr=config.from_config.pool_address,
                                                                           end_point=config.from_config.rpc.end_point,
                                                                           start=config.from_config.rpc.start,
                                                                           end=config.from_config.rpc.end,
                                                                           save_path=config.to_config.save_path,
                                                                           batch_size=config.from_config.rpc.batch_size,
                                                                           auth_string=config.from_config.rpc.auth_string,
                                                                           http_proxy=config.from_config.rpc.http_proxy,
                                                                           keep_tmp_files=config.from_config.rpc.keep_tmp_files
                                                                           )
            if config.from_config.rpc.ignore_position_id:
                source_rpc.append_empty_proxy_log(raw_file_list)
            else:
                print_log("Pool logs has downloaded, now will download proxy logs")
                source_rpc.append_proxy_log(raw_file_list=raw_file_list,
                                            start_height=start,
                                            end_height=end,
                                            chain=config.from_config.chain,
                                            end_point=config.from_config.rpc.end_point,
                                            save_path=config.to_config.save_path,
                                            batch_size=config.from_config.rpc.batch_size,
                                            auth_string=config.from_config.rpc.auth_string,
                                            http_proxy=config.from_config.rpc.http_proxy,
                                            keep_tmp_files=config.from_config.rpc.keep_tmp_files
                                            )
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
            raw_file_list = source_file.convert_log_file_to_standard(config.from_config.file)
        case DataSource.chifra_log:
            raw_file_list = source_chifra_log.load_raw_files(config.from_config.file)
    print("\n")
    print_log(f"Download finish")
    if config.to_config.type != ToType.raw:
        print_log(f"Start generate {len(raw_file_list)} files")
        generate_to_files(config.to_config, raw_file_list)


def generate_one(param):
    file, to_config = param
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


def generate_to_files(to_config: ToConfig, raw_files: List[str]):
    if to_config.multi_process:
        cpu_count = os.cpu_count()
        files_with_config = [(x, to_config) for x in raw_files]
        with Pool(cpu_count) as p:
            list(tqdm(p.imap(generate_one, files_with_config), ncols=120, total=len(files_with_config)))
    else:
        with tqdm(total=len(raw_files), ncols=120) as pbar:
            for file in raw_files:
                generate_one((file, to_config))
                pbar.update()
