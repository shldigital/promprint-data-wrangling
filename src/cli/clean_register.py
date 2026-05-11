"""Clean register tables, removing nonalphanumeric characters and metadata."""

import lib.helpers as helpers
import logging
import pandas as pd

from pathlib import Path
from typing import Any

rename_dict = {
    "Register Year": "register",
    "Register Block": "block",
    "Page in PDF": "page",
    "Line number": "line",
    "Book Title": "title",
    "Publisher": "publisher",
}

additional_columns = ["creator", "clean_title", "clean_publisher"]

logger = logging.getLogger("")


def main(
    input_file: str, output_folder: str, debug: bool = False, **kwargs: Any
) -> None:
    """
    Clean register tables, removing nonalphanumeric characters and metadata.

    :param input_folder: Path to .csv file containing register data
    :type input_folder: str
    :param output_folder: Path to folder for saving output
    :type input_folder: str
    :type config_file: str
    :param debug: Turn on debug to save intermediate stages of data to file
    :type debug: bool
    :return: None
    """
    file_path = Path(input_file)
    df = pd.read_csv(file_path)

    expected_columns: list[str] = list(rename_dict.keys())
    if not all(name in df.columns for name in expected_columns):
        raise KeyError(
            "Input file does not have the expected columns:" f"{expected_columns}"
        )

    df = df.rename(columns=rename_dict)

    df = helpers.clean_titles(df, file_path, debug)
    df["clean_publisher"] = df["publisher"].astype(str).map(helpers.clean_text)

    do_entries = df.loc[df["clean_publisher"].astype(str) == "do"]
    for i in do_entries.index:
        df.at[i, "clean_publisher"] = df.iloc[i-1]["clean_publisher"]

    required_columns: list[str] = list(rename_dict.values()) + additional_columns
    df = df.reindex(columns=required_columns)
    df.index = df["register"] + ":" + df.index.astype(str)
    df.index.name = "id"

    new_name: Path = file_path.stem + "_export.csv"
    output_path = Path(output_folder)
    df.to_csv(output_path / new_name)
