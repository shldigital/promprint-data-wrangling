import pandas as pd
import pathlib
import pytest

from src.cli.clean_register import main, rename_dict, additional_columns
from typing import List

input_file = "./tests/test_register/test_register.csv"
bad_column_labels_file = "./tests/test_register/bad_column_labels.csv"
output_filename = "test_register_export.csv"


def test_returns_csv_file(tmp_path):
    main(input_file, tmp_path, False)
    assert pathlib.Path.exists(tmp_path / output_filename)


def test_raises_key_error_on_bad_columns(tmp_path):
    with pytest.raises(KeyError):
        main(bad_column_labels_file, tmp_path, False)


def test_has_required_columns(tmp_path):
    main(input_file, tmp_path, False)
    df: pd.DataFrame = pd.read_csv(tmp_path / output_filename)
    required_columns: List[str] = list(rename_dict.values()) + additional_columns
    assert all(name in df.columns for name in required_columns)


def test_titles_cleaned(tmp_path):
    main(input_file, tmp_path, False)
    df: pd.DataFrame = pd.read_csv(tmp_path / output_filename)
    expected_title = "gospel herald new series"
    assert df["clean_title"].iloc[0] == expected_title
