import logging
import pandas as pd

from lib.helpers import clean_titles
from pathlib import Path
from typing import Any

rename_dict = {
    "Register Year": "register",
    "Register Block": "block",
    "Page in PDF": "page",
    "Line number": "line",
    "Book Title": "title",
    "Publisher": "publisher"
}

additional_columns = ["creator", "clean_title"]

logger = logging.getLogger('')


def main(input_file: str, output_folder: str, debug: bool,
         **kwargs: Any) -> None:
    file_path = Path(input_file)
    df = pd.read_csv(file_path)

    expected_columns: list[str] = list(rename_dict.keys())
    if not all(name in df.columns for name in expected_columns):
        raise KeyError("Input file does not have the expected columns:"
                       f"{expected_columns}")

    df = df.rename(columns=rename_dict)

    df = clean_titles(df, file_path, debug)

    required_columns: list[str] = list(rename_dict.values()) + additional_columns
    df = df.reindex(columns=required_columns)
    df.index = df["register"] + ":" + df.index.astype(str)
    df.index.name = "id"

    new_name: Path = file_path.stem + '_export.csv'
    output_path = Path(output_folder)
    df.to_csv(output_path / new_name)
