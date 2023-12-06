import os.path
from typing import Dict

import pandas as pd

import demeter_fetch.processor_aave.minute as processor_minute
import demeter_fetch.processor_aave.tick as processor_tick
import demeter_fetch.source_big_query.aave as source_big_query
import demeter_fetch.source_file.common as source_file
import demeter_fetch.source_rpc.aave as source_rpc
from ._typing import Config, ToType, DataSource, AaveKey
from .general_downloader import GeneralDownloader
from .utils import print_log, convert_raw_file_name, TimeUtil, get_aave_file_name


def process_aave_raw_file(param):
    file, to_config = param
    target_file_name = convert_raw_file_name(file, to_config)

    raw_df = pd.read_csv(file)
    raw_df["block_timestamp"] = raw_df["block_timestamp"].apply(lambda x: x.split("+")[0])
    raw_df["block_timestamp"] = pd.to_datetime(raw_df["block_timestamp"])

    raw_df = raw_df.sort_values(["block_number", "log_index"], ascending=[True, True], ignore_index=True)

    match to_config.type:
        case ToType.minute:
            result_df = processor_minute.preprocess_one(raw_df)
        case ToType.tick:
            result_df = processor_tick.preprocess_one(raw_df)
        case _:
            raise NotImplementedError(f"Convert to {to_config.type} not implied")
    result_df.to_csv(target_file_name)


class Downloader(GeneralDownloader):
    def _get_process_func(self):
        return process_aave_raw_file

    def _download_rpc(self, config: Config):
        raise NotImplementedError()

    def _download_big_query(self, config: Config):
        return source_big_query.download_event(
            config.from_config.chain,
            config.to_config.to_file_list,
            config.to_config.save_path,
            config.from_config.big_query.auth_file,
            config.from_config.http_proxy,
            config.to_config.type,
        )

    def _download_chifra(self, config: Config):
        raise NotImplementedError("Downloading aave data form chifra is not supported yet.")

    def _get_to_files(self, config) -> Dict:
        if config.from_config.data_source == DataSource.file:
            raw_list = source_file.load_raw_file_names(config.from_config.file)
            return {rf: convert_raw_file_name(rf, config.to_config) for rf in raw_list}

        days = []
        if config.from_config.data_source == DataSource.big_query:
            days = TimeUtil.get_date_array(config.from_config.big_query.start, config.from_config.big_query.end)
        elif config.from_config.data_source == DataSource.rpc:
            days = TimeUtil.get_date_array(config.from_config.rpc.start, config.from_config.rpc.end)

        to_file_list: Dict[AaveKey, str] = {}
        for day in days:
            for addr in config.from_config.aave_config.tokens:
                raw_file_name = get_aave_file_name(config.from_config.chain, addr, day)
                to_file_name = convert_raw_file_name(raw_file_name, config.to_config)
                to_file_list[AaveKey(day, addr)] = to_file_name
        return to_file_list
