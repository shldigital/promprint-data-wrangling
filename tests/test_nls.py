import glob
import re
import pandas as pd

from pathlib import Path
from src.lib.nls import add_file_data_to_index, columnise_nls_data


def test_file_id_prefix_added_to_index():
    input_folder = "./tests/test_files/test_nls/"
    file_path = Path(glob.glob(input_folder + '*.txt')[0])
    df = pd.read_csv(file_path, sep='\t')
    df = columnise_nls_data(df, file_path, False)
    original_index = df.index
    df = add_file_data_to_index(df, file_path)
    id = re.search(r"(\d{2})\.txt", str(file_path)).group(1)
    updated = map(lambda x, y: x == f'{id}:{y}', df.index, original_index)
    assert all(updated)


def test_no_file_number_falls_back_to_filename_as_id():
    input_folder = "./tests/test_files/test_nls_no_number/"
    file_path = Path(glob.glob(input_folder + '*.txt')[0])
    df = pd.read_csv(file_path, sep='\t')
    df = columnise_nls_data(df, file_path, False)
    original_index = df.index
    df = add_file_data_to_index(df, file_path)
    id = "test_nls_sample"
    updated = map(lambda x, y: x == f'{id}:{y}', df.index, original_index)
    assert all(updated)
