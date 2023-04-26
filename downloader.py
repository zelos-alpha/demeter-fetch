import os
from typing import List, Tuple

from _typing import *
import pandas as pd
import tick_processor


def download(config: Config):
    raw_file_list = []
    proxy_path = ""
    match config.from_config.data_source:
        case DataSource.rpc:
            raise NotImplementedError()  # TODO : rpc可以通过查询浏览器的api, 自动从时间获取高度
        case DataSource.big_query:
            raise NotImplementedError()
        case DataSource.file:
            raw_file_list, proxy_path = load_raw_files(config.from_config.file)

    generate_to_files(config.to_config, raw_file_list, proxy_path)


def load_raw_files(file_config: FileConfig) -> Tuple[List[str], str | None]:
    if file_config.file_path and file_config.proxy_file_path:
        print("both file and folder is specified, will process file")
    file_list = []
    if file_config.file_path:
        file_list = [file_config.file_path]
    elif file_config.folder:
        file_list = os.listdir(file_config.folder)
        file_list = filter(lambda e: e.endswith(".csv") or e.endswith(".CSV"), file_list)
        file_list = list(map(lambda e: os.path.join(file_config.folder, e), file_list))
    proxy_file_path = file_config.proxy_file_path
    return file_list, proxy_file_path


def generate_to_files(to_config: ToConfig, raw_files: List[str], proxy_path: str | None):
    for file in raw_files:
        df = pd.read_csv(file)
        df = df.rename(columns={
            "log_index": "pool_log_index",
            "transaction_index": "pool_tx_index",
            "DATA": "pool_data",
            "topics": "pool_topics",
        })
        result_df = pd.DataFrame()
        match to_config.type:
            case ToType.minute:
                raise NotImplementedError()
            case ToType.tick:
                result_df = tick_processor.preprocess_one(df)
        file_name = os.path.basename(file)
        file_name_and_ext = os.path.splitext(file_name)
        result_df.to_csv(os.path.join(to_config.save_path, file_name_and_ext[0] + ".processed" + file_name_and_ext[1]), index=False)
