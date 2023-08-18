import os
from typing import List

import demeter_fetch._typing as _typing
from demeter_fetch.utils import print_log


def load_raw_file_names(file_config: _typing.FileConfig) -> List[str]:
    if file_config.files and file_config.folder:
        print_log("both file and folder is specified, will process file")
    file_list = []
    if file_config.files and len(file_config.files) > 0:
        file_list = file_config.files
    elif file_config.folder:
        file_list = os.listdir(file_config.folder)
        file_list = filter(lambda e: e.endswith(".raw.csv"), file_list)
        file_list = list(map(lambda e: os.path.join(file_config.folder, e), file_list))

    return file_list
