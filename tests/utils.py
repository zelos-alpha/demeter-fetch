import hashlib
import os

import pandas as pd


def validate_files_by_md5(file_pathes):
    for path in file_pathes:
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        with open(path, "rb") as f:
            file_hash_test = hashlib.md5()
            while chunk := f.read(8192):
                file_hash_test.update(chunk)
        test_hash = file_hash_test.hexdigest()
        sample_file = os.path.join("samples", os.path.basename(path))
        with open(sample_file, "rb") as f:
            file_hash_sample = hashlib.md5()
            while chunk := f.read(8192):
                file_hash_sample.update(chunk)
        sample_hash = file_hash_sample.hexdigest()
        assert test_hash == sample_hash
    pass


def validate_files_by_dataframe(file_pathes):
    for path in file_pathes:
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        df_test = pd.read_csv(path)
        sample_file = os.path.join("samples", os.path.basename(path))
        df_sample = pd.read_csv(sample_file)
        assert df_test.size == df_sample.size
        assert df_test.equals(df_sample)
    pass
