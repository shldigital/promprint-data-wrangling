import ast
import glob
import numpy as np
import pandas as pd
import pytest

from src.datawrangling.clean_nls import main
from src.config.nls import nls_config

input_folder = "./tests/test_files/test_nls/"
wrong_keys_config_file = "./tests/test_files/wrong_keys_config.py"
one_register_config_file = "./tests/test_files/one_register_test_config.py"


def test_returns_tsv_file(tmp_path):
    main(input_folder, tmp_path, False)
    outputs = glob.glob(str(tmp_path) + '/*.tsv')
    assert len(outputs) > 0


def test_empty_folder_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        main("./tests/empty_folder/", tmp_path, False)


def test_output_created_for_each_register(tmp_path):
    main(input_folder, tmp_path, False)
    outputs = glob.glob(str(tmp_path) + '/*.tsv')
    registers = list(nls_config["registers"].keys())
    present = []
    for output in outputs:
        present.append(any([output.find(register) for register in registers]))
    assert all(present)
