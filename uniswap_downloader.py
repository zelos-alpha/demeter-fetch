import os
from typing import List, Tuple

from _typing import *
import pandas as pd
import tick_processor
import source_big_query
import source_file


def download(config: Config):
    raw_file_list = []
    match config.from_config.data_source:
        case DataSource.rpc:
            raise NotImplementedError()  # TODO : rpc可以通过查询浏览器的api, 自动从时间获取高度
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
    # generate_to_files(config.to_config, raw_file_list)


def generate_to_files(to_config: ToConfig, raw_files: List[str]):
    for file in raw_files:
        df = pd.read_csv(file)
        # df = df.rename(columns={
        #     "log_index": "pool_log_index",
        #     "transaction_index": "pool_tx_index",
        #     "DATA": "pool_data",
        #     "topics": "pool_topics",
        # })
        result_df = pd.DataFrame()
        match to_config.type:
            case ToType.minute:
                raise NotImplementedError()
            case ToType.tick:
                result_df = tick_processor.preprocess_one(df)
        file_name = os.path.basename(file)
        file_name_and_ext = os.path.splitext(file_name)
        result_df.to_csv(os.path.join(to_config.save_path, file_name_and_ext[0] + ".processed" + file_name_and_ext[1]), index=False)
