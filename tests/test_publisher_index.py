import glob
import pandas as pd
import pytest

from pathlib import Path
from src.cli.publisher_index import main, expected_columns

test_register = Path("./tests/test_files/test_register_cleaned.csv")
no_publisher_column = Path("./tests/test_files/test_register_no_publisher.csv")


def test_returns_csv_file(tmp_path):
    main(test_register, tmp_path)
    outputs = glob.glob(str(tmp_path) + "/*.csv")
    assert len(outputs) == 1


def test_raises_key_error_on_bad_columns(tmp_path):
    with pytest.raises(KeyError):
        main(no_publisher_column, tmp_path)


def test_outputs_expected_columns(tmp_path):
    main(test_register, tmp_path)
    publishers_df = pd.read_csv(tmp_path / "publishers.csv")
    assert all(col in publishers_df.columns for col in expected_columns)



