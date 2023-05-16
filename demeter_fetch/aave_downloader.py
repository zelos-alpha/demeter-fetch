import os
import pandas as pd
from tqdm import tqdm
from ._typing import *
import demeter_fetch.source_rpc as source_rpc
from .utils import print_log
import demeter_fetch.processor_minute as processor_minute
import demeter_fetch.processor_tick as processor_tick


def download(config: Config):
    raw_file_list = []
    match config.from_config.data_source:
        case DataSource.rpc:
            raw_file_list, start, end = source_rpc.query_aave_pool_logs(
                chain=config.from_config.chain,
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
            print_log("Pool logs has downloaded, now will download proxy logs")
            source_rpc.append_proxy_file(
                raw_file_list=raw_file_list,
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

    print("\n")
    print_log(f"Download finish")
    if config.to_config.type != ToType.raw:
        print_log(f"Start generate {len(raw_file_list)} files")
        generate_to_files(config.to_config, raw_file_list)


def generate_to_files(to_config: ToConfig, raw_files: List[str]):
    with tqdm(total=len(raw_files), ncols=150) as pbar:
        for file in raw_files:
            df = pd.read_csv(file)
            result_df = pd.DataFrame()
            df = df.sort_values(['block_number', 'pool_log_index'], ascending=[True, True], ignore_index=True)

            match to_config.type:
                case ToType.minute:
                    result_df = processor_minute.preprocess_one(df)
                case ToType.tick:
                    result_df = processor_tick.preprocess_one(df)
            file_name = os.path.basename(file)
            file_name_and_ext = os.path.splitext(file_name)
            result_df.to_csv(os.path.join(to_config.save_path,
                                          f"{file_name_and_ext[0].replace('.raw', '')}.{to_config.type.name}{file_name_and_ext[1]}"),
                             index=False)
            pbar.update()
