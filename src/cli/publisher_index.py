"""Create a grouped index of publishers from a cleaned collection."""

import pandas as pd
from pathlib import Path

expected_columns = ["publisher", "clean_publisher"]


def main(collection_path: Path, output_folder: Path):
    """
    Create a grouped index of publishers from a cleaned collection.

    The index groups entities that have misspellings or known name changes such
    that common publishing entities may be referred to via single index.

    :param collection_path: Path to csv file containing register or catalog data
    :type collection_path: pathlib.Path
    :param output_folder: Path to folder where results will be save as csv
    :type collection_path: pathlib.Path
    """
    df = pd.read_csv(collection_path)
    if not all(name in df.columns for name in expected_columns):
        raise KeyError(f"Input file does not have relevant columns: {expected_columns}")
    publishers_df = df.filter(["publisher", "clean_publisher"], axis=1)
    publishers_df.to_csv(output_folder / "publishers.csv")
