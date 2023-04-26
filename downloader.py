from typing import List, Tuple

from _typing import *


def download(config: Config):
    raw_file_list = []
    proxy_path = ""
    match config.from_config.data_source:
        case DataSource.rpc:
            pass
        case DataSource.big_query:
            pass
        case DataSource.file:
            raw_file_list, proxy_path = load_raw_files(config.from_config.file)

    generate_to_files(config.to_config, raw_file_list,proxy_path)


def load_raw_files(file_config: FileConfig) -> Tuple[List[str], str]:
    pass

def generate_to_files(to_config: ToConfig, raw_files: List[str], proxy_path:str):
    pass
