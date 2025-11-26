import ast
import glob
import lib.helpers as helpers
import lib.nls as nls
import logging
import pandas as pd

from functools import partial
from pathlib import Path
from typing import Any

logger = logging.getLogger('')


def main(input_folder: str, output_folder: str, config_file: str, debug: bool,
         **kwargs: Any) -> None:

    with open(config_file) as data:
        config: dict = ast.literal_eval(data.read())
    registers: dict[str, int] = config["NLS"]["registers"]
    date_range: float = config["NLS"]["date_range"]

    file_paths: list[Path] = list(map(Path, glob.glob(input_folder + '*.txt')))
    if len(file_paths) < 1:
        raise FileNotFoundError(f"No data found in {input_folder}")
    aggregate_path = Path(Path(input_folder).stem + '.tsv')

    compiled_df = pd.DataFrame()

    # N.B. We're doing cleaning per file so that a human can inspect
    # debug output easier (per file instead of the entire compilation)
    for file_path in file_paths:
        print(f"Processing: {file_path}")
        # `on_bad_lines` deals with the errant tabs at end of nls data files
        df = pd.read_csv(file_path,
                         sep='\t',
                         engine='python',
                         on_bad_lines=partial(lambda line: line[:15]))
        df = (df.pipe(nls.columnise_nls_data,  # type: ignore[call-overload]
                      file_path=file_path,
                      debug=debug)
              .pipe(helpers.clean_titles,
                    file_path=file_path,
                    debug=debug)
              .pipe(nls.clean_nls_dates,
                    file_path=file_path,
                    debug=debug))
        compiled_df = pd.concat([compiled_df, df])

    print(f"Total No. of entries: {len(compiled_df)}")

    if debug:
        compiled_path: Path = helpers.labelled_file(Path(output_folder),
                                                    aggregate_path, 'compiled')
        compiled_df.to_csv(compiled_path, sep='\t')

    for register_name, register_date in registers.items():
        register_df = nls.filter_nls_date(compiled_df,
                                          register_date,
                                          date_range)

        if debug:
            register_path: Path = helpers.labelled_file(Path(output_folder),
                                                        aggregate_path,
                                                        'filtered_' + register_name)
            register_df.to_csv(register_path, sep='\t')

        print(f"No. of entries after filtering for register {register_name}"
              f": {len(register_df)}")

        source_library = 'NLS'
        register_df = helpers.format_library_set(register_df, None,
                                                 source_library, register_name)
        register_path: Path = helpers.labelled_file(Path(output_folder),
                                                    aggregate_path,
                                                    register_name + "_export")
        register_df.to_csv(register_path, sep='\t')
