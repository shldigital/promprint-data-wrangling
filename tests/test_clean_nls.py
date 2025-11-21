import ast
import glob
import numpy as np
import pandas as pd
import pytest

from src.cli.clean_nls import main

input_folder = "./tests/test_files/test_nls/"
config_file = "./tests/test_files/test_config.py"
bad_config_file = "./tests/test_files/bad_test_config.py"
one_register_config_file = "./tests/test_files/one_register_test_config.py"


def test_returns_tsv_file(tmp_path):
    main(input_folder, tmp_path, config_file, False)
    outputs = glob.glob(str(tmp_path) + '/*.tsv')
    assert len(outputs) > 0


def test_output_created_for_each_register(tmp_path):
    main(input_folder, tmp_path, config_file, False)
    outputs = glob.glob(str(tmp_path) + '/*.tsv')
    with open(config_file) as data:
        config = ast.literal_eval(data.read())
    registers = list(config["NLS"]["registers"].keys())
    present = []
    for output in outputs:
        present.append(any([output.find(register) for register in registers]))
    assert all(present)


def test_bad_config_raises(tmp_path):
    with pytest.raises(SyntaxError):
        main(input_folder, tmp_path, bad_config_file, False)


def test_date_range_in_output(tmp_path):
    # N.B. Our test register contains at least one entry for each
    # date in the range defined by `one_register_config_file`
    main(input_folder, tmp_path, one_register_config_file, False)
    output = glob.glob(str(tmp_path) + '/*.tsv')[0]
    df = pd.read_csv(output, sep='\t')
    with open(one_register_config_file) as data:
        config = ast.literal_eval(data.read())
    register_date = int(config["NLS"]["registers"]["test_register"])
    date_range = int(config["NLS"]["date_range"])
    dates = range(register_date - date_range, register_date + date_range + 1)
    dates = map(lambda d: str(d) + '-01-01', dates)
    dates = map(lambda d: str(np.datetime64(d)), dates)
    date_set = set(pd.concat([df["min_date"], df["max_date"]]))
    assert all(date in date_set for date in dates)
