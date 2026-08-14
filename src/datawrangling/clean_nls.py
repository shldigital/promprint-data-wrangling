"""Clean and filter data from National Library of Scotland collection."""

import glob
import lib.helpers as helpers
import lib.nls as nls
import logging
import pandas as pd

from src.config.nls import nls_config
from functools import partial
from pathlib import Path
from typing import Any

logger = logging.getLogger("")


def main(
    input_folder: str,
    output_folder: str,
    debug: bool = False,
    **kwargs: Any,
) -> None:
    """
    Clean and filter data from National Library of Scotland collection.

    :param input_folder: Path to folder containing tab separated .txt
      files that make up the NLS collection.
    :type input_folder: str
    :param output_folder: Path to folder for saving output
    :type input_folder: str
    :param debug: Turn on debug to save intermediate stages of data to file
    :type debug: bool
    :return: None
    """
    Path(output_folder).mkdir(parents=False, exist_ok=True)
    registers: dict[str, int] = nls_config["registers"]
    date_range: float = nls_config["date_range"]

    file_paths: list[Path] = list(map(Path, glob.glob(input_folder + "*.txt")))
    if len(file_paths) < 1:
        raise FileNotFoundError(f"No data found in {input_folder}")
    aggregate_path = Path(Path(input_folder).stem + ".tsv")

    section_list = []

    for file_path in file_paths:
        print(f"Processing: {file_path}")
        # `on_bad_lines` deals with the errant tabs at end of nls data files
        df = pd.read_csv(
            file_path,
            sep="\t",
            engine="python",
            on_bad_lines=partial(lambda line: line[:15]),
        )
        try:
            df = df.pipe(
                nls.columnise_nls_data,  # type: ignore[call-overload]
                file_path=file_path,
                debug=debug,
            ).pipe(nls.add_file_data_to_index, file_path=file_path)
            section_list.append(df)
        except AttributeError:
            print(f"Badly formed file at: {file_path}")
    compiled_df: pd.DataFrame = pd.concat(section_list)

    compiled_df = compiled_df.pipe(
        helpers.clean_titles, file_path=file_path, debug=debug
    ).pipe(nls.clean_nls_dates, file_path=file_path, debug=debug)

    compiled_df["clean_publisher"] = compiled_df["publisher"].map(helpers.clean_text)
    compiled_df["clean_creator"] = compiled_df["creator"].map(helpers.clean_text)
    compiled_df = compiled_df.drop_duplicates(
        subset=["clean_title", "clean_publisher", "clean_creator"]
    )

    print(f"Total No. of entries: {len(compiled_df)}")

    if debug:
        compiled_path: Path = helpers.labelled_file(
            Path(output_folder), aggregate_path, "compiled"
        )
        compiled_df.to_csv(compiled_path, sep="\t")

    for register_name, register_date in registers.items():
        register_df = nls.filter_nls_date(compiled_df, register_date, date_range)

        if debug:
            register_path: Path = helpers.labelled_file(
                Path(output_folder), aggregate_path, "filtered_" + register_name
            )
            register_df.to_csv(register_path, sep="\t")

        print(
            f"No. of entries after filtering for register {register_name}"
            f": {len(register_df)}"
        )

        source_library = "NLS"
        register_df = helpers.format_library_set(
            register_df, None, source_library, register_name
        )
        register_path: Path = helpers.labelled_file(
            Path(output_folder), aggregate_path, register_name + "_export"
        )
        register_df.to_csv(register_path, sep="\t")
