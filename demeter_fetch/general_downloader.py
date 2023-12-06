import os
from multiprocessing import Pool

from tqdm import tqdm

from . import source_file
from ._typing import *
from .utils import print_log
import demeter_fetch.source_chifra.chifra_utils as chifra_utils


def _generate_one(param):
    pass


class GeneralDownloader(object):
    def _download_rpc(self, config: Config):
        return []

    def _download_big_query(self, config: Config):
        return []

    def _download_chifra(self, config: Config):
        chifra_config = config.from_config.chifra_config
        if chifra_config.file_path is None or chifra_config.file_path == "":
            export_commend = chifra_utils.get_export_commend(config.from_config)
            print("Please export event logs via: ")
            print(export_commend)
            print("If the file has exported, input file path. ")
            answer_path = input(
                "Or press enter to exit and download later. Before next start, you can fill the path in config file to get start immediately"
            )
            if answer_path != "":
                chifra_config.file_path = answer_path
            else:
                return []
        else:
            print("Parameter file_path is filled, start/end parameter will be omitted")
        print_log(f"Will convert file {chifra_config.file_path}")

    def _get_process_func(self):
        def func(param):
            pass

        return func

    def download(self, config: Config):
        raw_file_list = []
        match config.from_config.data_source:
            case DataSource.rpc:
                raw_file_list = self._download_rpc(config)
                #     merge
            case DataSource.big_query:
                raw_file_list = self._download_big_query(config)
            case DataSource.file:
                raw_file_list = config.to_config.to_file_list.keys()
            case DataSource.chifra:
                raw_file_list = self._download_chifra(config)

        print("\n")
        print_log(f"Download finish")
        if config.to_config.type != ToType.raw:
            print_log(f"Start generate {len(raw_file_list)} files")
            GeneralDownloader._generate_to_files(config.to_config, raw_file_list, self._get_process_func())

    @staticmethod
    def _generate_to_files(to_config: ToConfig, raw_files: List[str], func_generate_one):
        if to_config.multi_process:
            cpu_count = os.cpu_count()
            files_with_config = [(x, to_config) for x in raw_files]
            with Pool(cpu_count) as p:
                list(
                    tqdm(
                        p.imap(func_generate_one, files_with_config),
                        ncols=120,
                        total=len(files_with_config),
                    )
                )
        else:
            with tqdm(total=len(raw_files), ncols=120) as pbar:
                for file in raw_files:
                    func_generate_one((file, to_config))
                    pbar.update()

    def _get_to_files(self, config: Config) -> Dict:
        return {}

    def set_file_list(self, config: Config) -> Config:
        all_files = self._get_to_files(config)
        total_file_count = len(all_files)
        print_log(f"Will generate {total_file_count} files")

        if not config.to_config.skip_existed:
            config.to_config.to_file_list = all_files
            return config
        should_download = {}
        for k, v in all_files.items():
            if not os.path.exists(v):
                should_download[k] = v
        config.to_config.to_file_list = should_download
        print_log(f"Skip existed files, {total_file_count-len(should_download)} files is exist, now will generate {len(should_download)} files")

        return config
