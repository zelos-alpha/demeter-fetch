import os
from typing import List, Tuple

import demeter_fetch._typing as _typing
from .utils import print_log


def load_raw_files(file_config: _typing.FileConfig) -> Tuple[List[str], str | None]:
    if file_config.files and file_config.proxy_file_path:
        print_log("both file and folder is specified, will process file")
    file_list = []
    if file_config.files and len(file_config.files) > 0:
        file_list = file_config.files
    elif file_config.folder:
        file_list = os.listdir(file_config.folder)
        file_list = filter(lambda e: e.endswith(".csv") or e.endswith(".CSV"), file_list)
        file_list = list(map(lambda e: os.path.join(file_config.folder, e), file_list))
    proxy_file_path = file_config.proxy_file_path
    return file_list, proxy_file_path
