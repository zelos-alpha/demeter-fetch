import os
from multiprocessing import Pool

from tqdm import tqdm

from ._typing import *
from .utils import print_log


def _generate_one(param):
    pass


class GeneralDownloader(object):
    def _download_rpc(self, config: Config):
        return []

    def _download_big_query(self, config: Config):
        return []

    def _download_file(self, config: Config):
        return []

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
                raw_file_list = self._download_file(config)

        print("\n")
        print_log(f"Download finish")
        if config.to_config.type != ToType.raw:
            print_log(f"Start generate {len(raw_file_list)} files")
            self._generate_to_files(config.to_config, raw_file_list, self._get_process_func())

    def _generate_to_files(
        self, to_config: ToConfig, raw_files: List[str], func_generate_one
    ):
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
