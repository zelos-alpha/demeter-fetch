import os
from multiprocessing import Pool
from typing import List

from tqdm import tqdm

from ._typing import Config, DataSource, ToType, ToConfig
from .general_downloader import GeneralDownloader
from .utils import print_log

class Downloader(GeneralDownloader):
    pass
