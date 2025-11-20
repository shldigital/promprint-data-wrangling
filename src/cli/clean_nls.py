import argparse
import glob
import lib.helpers as helpers
import lib.nls as nls
import logging
import pandas as pd

from functools import partial
from pathlib import Path
from typing import Any

logger = logging.getLogger('')
logging.basicConfig(level=logging.INFO,
                    filename="promprint-cleaning.log",
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filemode='w')

console = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)


def main(input_folder: str, output_folder: str, debug: bool,
         **kwargs: Any) -> None:
    file_paths = map(Path, glob.glob(input_folder + '*.tsv'))
    aggregate_path = Path(Path(input_folder).stem + '.tsv')

    registers = {"1863b": 1863, "undated": None}
    date_range = 1.
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
        compiled_path = helpers.labelled_file(Path(output_folder),
                                              aggregate_path, 'compiled')
        compiled_df.to_csv(compiled_path, sep='\t', index=False)

    for register_name, register_date in registers.items():
        register_df = nls.filter_nls_date(compiled_df,
                                          register_date,
                                          date_range)

        if debug:
            register_path = helpers.labelled_file(Path(output_folder),
                                                  aggregate_path,
                                                  'filtered_' + register_name)
            register_df.to_csv(register_path, sep='\t', index=False)

        print(f"No. of entries after filtering for register {register_name}"
              f": {len(register_df)}")

        source_library = 'NLS'
        register_df = helpers.format_library_set(register_df, None,
                                                 source_library, register_name)
        register_path = helpers.labelled_file(Path(output_folder),
                                              aggregate_path,
                                              register_name + "_export")
        register_df.to_csv(register_path, sep='\t', index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('input_folder',
                        help='Folder of input files in tsv format')
    parser.add_argument('output_folder', help='Folder of output files')
    parser.add_argument('-d',
                        '--debug',
                        action='store_true',
                        help='Save intermediate stages of cleaning to file')

    args = parser.parse_args()

    if args.debug:
        console.setLevel(logging.INFO)
    else:
        console.setLevel(logging.WARNING)
    logging.getLogger('').addHandler(console)

    main(args.input_folder, args.debug)
