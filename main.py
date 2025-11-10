import argparse
from functools import partial
import glob
import logging
import numpy as np
import os
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    filename="promprint-cleaning.log",
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filemode='w')

console = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)


def _labelled_file(out_dir: os.PathLike, file_path: os.PathLike,
                   label: str) -> os.PathLike:
    """
    Insert a text label into a filename and append to directory
    """
    new_name = file_path.stem + '_' + label + '.tsv'
    return out_dir / new_name


def clean_nls_dates(df: pd.DataFrame, file_path: os.PathLike,
                    filter_date: float,
                    debug: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean and process the dates of the dataset format provided by
    National Library of Scotland. This is 'dictionary' style format, with tab
    separated sets of key-value pairs for each entry. Each entry is then
    separated by new lines, e.g.

    Title: {first entry title}<tab>Creator: {entry creator}<tab>...<newline>
    Title: {second entry title}<tab>Creator: {second entry creator}<tab>...<newline>

    Saves data to tsv file in columnar format (one column per key) e.g.

    Title<tab>Creator<tab>...
    {first entry title}<tab>{second entry creator}<tab>...<newline>
    {second entry title}<tab>{second entry creator}<tab>...<newline>

    and also returns the formatted dataframe.

    :param df: The uncleaned dataframe in format provided by
               National Library of Scotland
    :param file_path: File path of original data, to name output files with
    :param filter_date: Date in year format (e.g. 1863) to filter, +/- 1 year
    :return pd.DataFrame: Cleaned entries, filtered down by date
    :return pd.DataFrame: Cleaned entries that have no interpretable date
    """

    out_dir = file_path.parent.joinpath(file_path.stem + "_clean")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Strip out the data key, but leave other colons found in value
    df = df.map(lambda x: ':'.join(x.split(':')[1:]).rstrip('/').strip())
    labels = [
        'Title', 'Creator', 'Type', 'Publisher', 'Date', 'Language', 'Format',
        'Relation', 'Rights', 'Identifier', 'Description', 'Subject',
        'Coverage', 'Contributor', 'Source'
    ]
    df.columns = labels
    if debug:
        df.to_csv(_labelled_file(out_dir, file_path, 'columnar'),
                  sep='\t',
                  index=False)

    df_len = len(df)
    logging.info(f'No. of entries: {df_len}')

    # Separate out different types of date in case they're relevant
    dates_re = (r'(?:c(?:a\.?|irca|) ?(?P<circa_date>\d{4})|'
                r'(?P<question_date>\d{4})\?|'
                r'(?P<unqualified_date>\d{4}))')
    dates_df = df['Date'].str.extractall(dates_re).astype('float64')
    if debug:
        dates_df.to_csv(
            _labelled_file(out_dir, file_path, 'dates'),
            sep='\t',
        )

    question_dates = dates_df.pop('question_date').groupby(
        level=0).first().dropna()
    n_qd = len(question_dates)
    logging.info(f'No. of question marked dates: {n_qd}')

    circa_dates = dates_df.pop('circa_date').groupby(level=0).first().dropna()
    n_cd = len(circa_dates)
    logging.info(f'No. of circa marked dates: {n_cd}')

    min_uq_dates = dates_df.groupby(level=0).min().rename(
        columns={
            'unqualified_date': 'min_uq_date'
        }).dropna()
    max_uq_dates = dates_df.groupby(level=0).max().rename(
        columns={
            'unqualified_date': 'max_uq_date'
        }).dropna()

    processed_dates = pd.DataFrame(
        np.nan,
        index=range(df_len),
        columns=['question_date', 'circa_date', 'min_uq_date', 'max_uq_date'])
    processed_dates.update(question_dates)
    processed_dates.update(circa_dates)
    processed_dates.update(min_uq_dates['min_uq_date'])
    processed_dates.update(max_uq_dates['max_uq_date'])

    date_range = pd.DataFrame(np.nan,
                              index=range(df_len),
                              columns=['min_date', 'max_date'])

    # NB: Effectively ignoring different date types for now
    date_range['min_date'] = processed_dates.min(axis=1)
    date_range['max_date'] = processed_dates.max(axis=1)

    processed_dates = processed_dates.join(date_range)
    # processed_dates = processed_dates.map(
    #     lambda x: pd.to_datetime(x, format='%Y', errors='coerce'))

    if debug:
        processed_dates.to_csv(
            _labelled_file(out_dir, file_path, 'processed_dates'),
            sep='\t',
        )

    df = pd.concat(
        [df.loc[:, :'Date'], processed_dates, df.loc[:, 'Language':]], axis=1)

    df.to_csv(_labelled_file(out_dir, file_path, 'clean'),
              sep='\t',
              index=False)

    # TODO: split function here
    register_df = df.loc[((df['min_date'] - 1.1) < filter_date)
                         & ((df['max_date'] + 1.1) > filter_date)]
    register_df.to_csv(_labelled_file(out_dir, file_path,
                                      'filtered_' + str(filter_date)),
                       sep='\t',
                       index=False)

    missing_df = df.loc[df['min_date'].isnull()]
    missing_df.to_csv(_labelled_file(out_dir, file_path, 'missing'),
                      sep='\t',
                      index=False)

    n_exact = len(df.loc[((df['min_date'] - 0.9) < filter_date)
                         & ((df['max_date'] + 0.9) > filter_date)])
    n_extended = len(register_df)
    n_missing = len(missing_df)
    logging.info(f'No. of missing/unrecognised dates: {n_missing}')
    logging.info(f"No. of entries filtered for date {filter_date} "
                 f"(exact, extended): {n_exact, n_extended}")
    return register_df.reindex(), missing_df.reindex()


def prepare_for_import(df: pd.DataFrame, to_datetime: bool,
                       debug: bool) -> pd.DataFrame:
    """
    Insert and rename required columns to match promprint database schema
    """

    df_len = len(df)
    df.columns = df.columns.str.lower()
    df = df.loc[:, ['title', 'creator', 'min_date', 'max_date']]
    df = df.rename(columns={'creator': 'author'})

    # Don't need old index info, reset it to match new columns
    df = df.reset_index(drop=True)
    df['id'] = pd.Series(np.nan, index=range(df_len))
    df['source_library'] = pd.Series(['NLS'] * df_len)
    df['register'] = pd.Series(['1863b'] * df_len)
    if to_datetime:
        df.loc[:,
               ["min_date", "max_date"
                ]] = df.loc[:, ["min_date", "max_date"]].map(
                    lambda x: pd.to_datetime(x, format='%Y', errors='coerce'))

    return df


def main(folder: str, debug: bool) -> None:

    file_paths = map(Path,
                     glob.glob(folder + '*.tsv') + glob.glob(folder + '*.txt'))
    register_date = 1863
    register_df = pd.DataFrame()
    missing_df = pd.DataFrame()
    for file_path in file_paths:
        print(f"Processing: {file_path}")
        df = pd.read_csv(file_path,
                         sep='\t',
                         engine='python',
                         on_bad_lines=partial(lambda line: line[:15]))
        try:
            new_register_df, new_missing_df = clean_nls_dates(df,
                                                              file_path,
                                                              register_date,
                                                              debug=debug)
            register_df = pd.concat([register_df, new_register_df])
            missing_df = pd.concat([missing_df, new_missing_df])
        except Exception as e:
            logging.error(f"Exception while processing {file_path},\n{e}")

    if debug:
        register_path = Path(folder).parent.joinpath(
            folder.rstrip("/") + "_filtered_" + str(register_date) + ".tsv")
        register_df.to_csv(register_path, sep='\t', index=False)

        missing_path = Path(folder).parent.joinpath(
            folder.rstrip("/") + "_missing.tsv")
        missing_df.to_csv(missing_path, sep='\t', index=False)

    print(f"No. of entries after filtering (extended): {len(register_df)}")
    print(f"No. of entries with no date: {len(missing_df)}")

    register_df = prepare_for_import(register_df,
                                     to_datetime=True,
                                     debug=debug)
    missing_df = prepare_for_import(missing_df, to_datetime=False, debug=debug)

    register_path = Path(folder).parent.joinpath(
        folder.rstrip("/") + "_filtered_" + str(register_date) + "_db.tsv")
    register_df.to_csv(register_path, sep='\t', index=False)

    missing_path = Path(folder).parent.joinpath(
        folder.rstrip("/") + "_missing_db.tsv")
    missing_df.to_csv(missing_path, sep='\t', index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('folder', help='Folder of input files in tsv format')
    parser.add_argument('--debug',
                        action='store_true',
                        help='Save intermediate stages of cleaning to file')

    args = parser.parse_args()

    if args.debug:
        console.setLevel(logging.INFO)
    else:
        console.setLevel(logging.WARNING)
    logging.getLogger('').addHandler(console)

    main(args.folder, args.debug)
