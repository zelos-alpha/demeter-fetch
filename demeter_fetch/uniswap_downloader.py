from datetime import timedelta
from typing import Tuple

import pandas as pd

import demeter_fetch.processor_uniswap.minute as processor_minute
import demeter_fetch.processor_uniswap.tick as processor_tick
import demeter_fetch.processor_uniswap.position as processor_position
import demeter_fetch.source_big_query.uniswap as source_big_query
import demeter_fetch.source_chifra.uniswap as source_chifra
import demeter_fetch.source_file.common as source_file
import demeter_fetch.source_rpc.uniswap as source_rpc
from demeter_fetch.constants import PROXY_CONTRACT_ADDRESS
from ._typing import *
from .general_downloader import GeneralDownloader
from .utils import print_log, convert_raw_file_name, TimeUtil, get_file_name


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
        case ToType.position:
            result_df = processor_tick.preprocess_one(df)
        case _:
            raise NotImplementedError(f"Convert to {to_config.type} not implied")
    result_df.to_csv(target_file_name, index=False)


def generate_position(config):
    processor_position.generate_user_position(config)


class Downloader(GeneralDownloader):
    def _get_process_func(self):
        return generate_one

    def generate_position(self, config):
        generate_position(config)

    def _download_rpc(self, config: Config):
        if len(config.to_config.to_file_list) == 0:
            return []
        continuous_day: List[Tuple[date, date]] = []
        # find continuous days
        tmp_start_day = tmp_yesterday = None
        for day in config.to_config.to_file_list.keys():
            if tmp_yesterday is not None and day != tmp_yesterday + timedelta(days=1):
                continuous_day.append((tmp_start_day, tmp_yesterday))
                tmp_start_day = None
            if tmp_start_day is None:
                tmp_start_day = day
            tmp_yesterday = day
        continuous_day.append((tmp_start_day, tmp_yesterday))

        all_raw_files = []
        for day_range in continuous_day:
            print(f"Download from {day_range[0]} to {day_range[1]}")
            raw_file_list, start_height, end_height = source_rpc.query_uniswap_pool_logs(
                chain=config.from_config.chain,
                pool_addr=config.from_config.uniswap_config.pool_address,
                end_point=config.from_config.rpc.end_point,
                start=day_range[0],
                end=day_range[1],
                save_path=config.to_config.save_path,
                batch_size=config.from_config.rpc.batch_size,
                auth_string=config.from_config.rpc.auth_string,
                http_proxy=config.from_config.http_proxy,
                keep_tmp_files=config.from_config.rpc.keep_tmp_files,
                etherscan_api_key=config.from_config.rpc.etherscan_api_key,
            )
            if config.from_config.rpc.ignore_position_id:
                source_rpc.append_empty_proxy_log(raw_file_list)
            else:
                print_log("Pool logs has downloaded, now will download proxy logs")
                source_rpc.append_proxy_log(
                    raw_file_list=raw_file_list,
                    start_height=start_height,
                    end_height=end_height,
                    chain=config.from_config.chain,
                    end_point=config.from_config.rpc.end_point,
                    save_path=config.to_config.save_path,
                    batch_size=config.from_config.rpc.batch_size,
                    auth_string=config.from_config.rpc.auth_string,
                    http_proxy=config.from_config.http_proxy,
                    keep_tmp_files=config.from_config.rpc.keep_tmp_files,
                )
            all_raw_files.extend(raw_file_list)

        return all_raw_files

    def _download_big_query(self, config: Config):
        if config.to_config.type == ToType.position:
            source_big_query.download_event(
                config.from_config.chain,
                ChainTypeConfig[config.from_config.chain]["uniswap_proxy_addr"],
                source_big_query.download_proxy_event_one_day,
                config.to_config.to_file_list,
                config.to_config.save_path,
                config.from_config.big_query.auth_file,
                config.from_config.http_proxy,
            )
        return source_big_query.download_event(
            config.from_config.chain,
            config.from_config.uniswap_config.pool_address,
            source_big_query.download_pool_event_one_day,
            config.to_config.to_file_list,
            config.to_config.save_path,
            config.from_config.big_query.auth_file,
            config.from_config.http_proxy,
        )

    def _download_chifra(self, config: Config):
        super()._download_chifra(config)
        if config.from_config.chifra_config.start and config.from_config.chifra_config.end:
            contract_info = {
                config.from_config.uniswap_config.pool_address: config.from_config.chifra_config.file_path,
                PROXY_CONTRACT_ADDRESS: config.from_config.chifra_config.proxy_file_path
            }
            for contract, save_path in contract_info.items():
                source_chifra.download_event(
                    config.from_config.chain,
                    contract,
                    config.to_config.to_file_list,
                    save_path,
                    config.from_config.chifra_config.etherscan_api_key,
                    config.from_config.http_proxy,
                )
        return source_chifra.convert_chifra_csv_to_raw_file(config)

    def _get_to_files(self, config: Config) -> Dict:
        if config.from_config.data_source == DataSource.file:
            raw_list = source_file.load_raw_file_names(config.from_config.file)
            return {rf: convert_raw_file_name(rf, config.to_config) for rf in raw_list}

        days = []
        if config.from_config.data_source == DataSource.big_query:
            days = TimeUtil.get_date_array(config.from_config.big_query.start, config.from_config.big_query.end)
        elif config.from_config.data_source == DataSource.rpc:
            days = TimeUtil.get_date_array(config.from_config.rpc.start, config.from_config.rpc.end)
        elif config.from_config.data_source == DataSource.chifra:
            if config.from_config.chifra_config.start and config.from_config.chifra_config.end:
                days = TimeUtil.get_date_array(config.from_config.chifra_config.start, config.from_config.chifra_config.end)

        to_file_list: Dict[date, str] = {}
        for day in days:
            raw_file_name = get_file_name(config.from_config.chain, config.from_config.uniswap_config.pool_address, day)
            to_file_name = convert_raw_file_name(raw_file_name, config.to_config)
            to_file_list[day] = to_file_name
        return to_file_list
