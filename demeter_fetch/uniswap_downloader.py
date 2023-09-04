import os

import pandas as pd

import demeter_fetch.processor_uniswap.minute as processor_minute
import demeter_fetch.processor_uniswap.tick as processor_tick
import demeter_fetch.source_big_query.uniswap as source_big_query
import demeter_fetch.source_file.common as source_file
import demeter_fetch.source_rpc.uniswap as source_rpc
from ._typing import *
from .general_downloader import GeneralDownloader
from .utils import print_log, convert_raw_file_name


def generate_one(param):
    file, to_config = param

    target_file_name = convert_raw_file_name(file, to_config)

    df = pd.read_csv(file)
    # df = df.rename(columns={
    #     "log_index": "pool_log_index",
    #     "transaction_index": "pool_tx_index",
    #     "DATA": "pool_data",
    #     "topics": "pool_topics",
    # })
    df = df.sort_values(["block_number", "pool_log_index"], ascending=[True, True], ignore_index=True)

    match to_config.type:
        case ToType.minute:
            result_df = processor_minute.preprocess_one(df)
        case ToType.tick:
            result_df = processor_tick.preprocess_one(df)
        case _:
            raise NotImplementedError(f"Convert to {to_config.type} not implied")
    result_df.to_csv(target_file_name, index=False)


class Downloader(GeneralDownloader):
    def _get_process_func(self):
        return generate_one

    def _download_rpc(self, config: Config):
        raw_file_list, start, end = source_rpc.query_uniswap_pool_logs(
            chain=config.from_config.chain,
            pool_addr=config.from_config.uniswap_config.pool_address,
            end_point=config.from_config.rpc.end_point,
            start=config.from_config.rpc.start,
            end=config.from_config.rpc.end,
            save_path=config.to_config.save_path,
            batch_size=config.from_config.rpc.batch_size,
            auth_string=config.from_config.rpc.auth_string,
            http_proxy=config.from_config.rpc.http_proxy,
            keep_tmp_files=config.from_config.rpc.keep_tmp_files,
        )
        if config.from_config.rpc.ignore_position_id:
            source_rpc.append_empty_proxy_log(raw_file_list)
        else:
            print_log("Pool logs has downloaded, now will download proxy logs")
            source_rpc.append_proxy_log(
                raw_file_list=raw_file_list,
                start_height=start,
                end_height=end,
                chain=config.from_config.chain,
                end_point=config.from_config.rpc.end_point,
                save_path=config.to_config.save_path,
                batch_size=config.from_config.rpc.batch_size,
                auth_string=config.from_config.rpc.auth_string,
                http_proxy=config.from_config.rpc.http_proxy,
                keep_tmp_files=config.from_config.rpc.keep_tmp_files,
            )
        return raw_file_list

    def _download_big_query(self, config: Config):
        return source_big_query.download_event(
            config.from_config.chain,
            config.from_config.uniswap_config.pool_address,
            config.from_config.big_query.start,
            config.from_config.big_query.end,
            config.to_config.save_path,
            config.from_config.big_query.auth_file,
            config.from_config.big_query.http_proxy,
        )

    def _download_file(self, config: Config):
        return source_file.load_raw_file_names(config.from_config.file)
