import argparse
import glob
import logging
import numpy as np
import os
import pandas as pd
import re

from functools import partial
from pathlib import Path
from typing import List, Tuple

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


def _remove_metadata(title_string: str) -> str:
    """
    Remove strings and numbers not directly related to the title of the entry
    """
    square_brackets_clean = re.sub(
        r'\[(?:microform|illustrated|a novel|plates)\]', '',
        title_string.lower())
    editions_clean = re.sub(r'\b(?:n|ed|vol(?:s|ume|umes|))\b', '',
                            square_brackets_clean)
    return re.sub(r'\d{1,4}', '', editions_clean)


def _clean_title_string(title_string: str) -> str:
    """
    Remove/replace ampersands, apostrophes and multi-spaces
    """
    no_ampersand = re.sub(r'(&amp;|&)', 'and', title_string)
    no_apostrophe = re.sub(r"['`]", '', no_ampersand)
    alphanum = re.sub(r'[^a-zA-Z0-9]', ' ', no_apostrophe)
    single_spaced = re.sub(r'\s{2,}', ' ', alphanum)
    return single_spaced.strip().lower()


def columnise_nls_data(df: pd.DataFrame, file_path: os.PathLike,
                       debug: bool) -> pd.DataFrame:
    """
    Reformat data provided by National Library of Scotland.
    This is 'dictionary' style format, with tab separated sets of key-value
    pairs for each entry. Each entry is then separated by new lines, e.g.

    Title: {first entry title}<tab>Creator: {entry creator}<tab>...<newline>
    Title: {second entry title}<tab>Creator: {second entry creator}<tab>...<newline>

    Returns a dataframe in columnar format (one column per key) e.g.

    Title<tab>Creator<tab>...
    {first entry title}<tab>{second entry creator}<tab>...<newline>
    {second entry title}<tab>{second entry creator}<tab>...<newline>

    :param df: The dataframe in format provided by National Library of Scotland
    :param file_path: File path of original data, to name debug output files
    :param debug: if True then save the dataframe out as a tsv file
    :return pd.DataFrame: The columnar dataframe
    """
    # Strip out the data key, but leave other colons found in value
    df = df.map(lambda x: ':'.join(x.split(':')[1:]))
    labels = [
        'Title', 'Creator', 'Type', 'Publisher', 'Date', 'Language', 'Format',
        'Relation', 'Rights', 'Identifier', 'Description', 'Subject',
        'Coverage', 'Contributor', 'Source'
    ]
    df.columns = labels
    if debug:
        out_dir = file_path.parent.joinpath(file_path.stem + "_clean")
        out_dir.mkdir(parents=True, exist_ok=True)
        df.to_csv(_labelled_file(out_dir, file_path, 'columnar'),
                  sep='\t',
                  index=False)
    return df


def clean_nls_titles(df: pd.DataFrame, file_path: os.PathLike,
                     debug: bool) -> pd.DataFrame:
    """
    Collecting the different title cleaning functions here

    :param df: The dataframe with uncleaned titles in columnar format
    :param file_path: File path of original data, to name debug output files
    :param debug: if True then save the dataframe out as a tsv file
    :return pd.DataFrame: The columnar dataframe
    """
    df['clean_title'] = (
        df['Title'].map(_remove_metadata).map(_clean_title_string))
    if debug:
        out_dir = file_path.parent.joinpath(file_path.stem + "_clean")
        out_dir.mkdir(parents=True, exist_ok=True)
        df.to_csv(_labelled_file(out_dir, file_path, 'clean_titles'),
                  sep='\t',
                  index=False)
    return df


def clean_nls_dates(df: pd.DataFrame, file_path: os.PathLike,
                    debug: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean the dates of the National Library of Scotland dataset.
    The dates have multiple annotations e,g, c1983, circa 1983 etc.
    Try to identify and group these and give columns for min and
    max dates

    :param df: The uncleaned dataframe in format provided by
               National Library of Scotland
    :param file_path: File path of original data, to name debug files
    :param debug: if True then save the dataframe out as a tsv file
    :return pd.DataFrame: Cleaned entries
    """

    df_len = len(df)

    # Separate out different types of date in case they're relevant
    dates_re = (r'(?:c(?:a\.?|irca|) ?(?P<circa_date>\d{4})|'
                r'(?P<question_date>\d{4})\?|'
                r'(?P<unqualified_date>\d{4}))')

    # create a dataframe of date matches with original index and match index
    # and match index as a multi-index
    dates_df = df['Date'].str.extractall(dates_re).astype('float64')
    if debug:
        out_dir = file_path.parent.joinpath(file_path.stem + "_clean")
        out_dir.mkdir(parents=True, exist_ok=True)
        dates_df.to_csv(
            _labelled_file(out_dir, file_path, 'datetypes'),
            sep='\t',
        )

    # Grab the question and circa dates with their original indices
    question_dates = dates_df.pop('question_date').groupby(
        level=0).first().dropna()
    n_qd = len(question_dates)

    circa_dates = dates_df.pop('circa_date').groupby(level=0).first().dropna()
    n_cd = len(circa_dates)

    # Grab the unqualified dates with original indices into one series,
    # if there are more than one, take the lowest
    min_uq_dates = dates_df.groupby(level=0).min().rename(
        columns={
            'unqualified_date': 'min_uq_date'
        }).dropna()

    # Grab the unqualified dates with original indices into one series,
    # if there are more than one, take the highest
    max_uq_dates = dates_df.groupby(level=0).max().rename(
        columns={
            'unqualified_date': 'max_uq_date'
        }).dropna()

    # Make a new empty dataframe to hold the sorted dates data
    # the new frame has the same size/index as the original
    processed_dates = pd.DataFrame(
        np.nan,
        index=range(df_len),
        columns=['question_date', 'circa_date', 'min_uq_date', 'max_uq_date'])

    # Insert the various dates at their labelled indices
    processed_dates.update(question_dates)
    processed_dates.update(circa_dates)
    processed_dates.update(min_uq_dates['min_uq_date'])
    processed_dates.update(max_uq_dates['max_uq_date'])

    # Make a new empty dataframe to hold the reduced dates data
    # the new frame has the same size/index as the original
    date_range = pd.DataFrame(np.nan,
                              index=range(df_len),
                              columns=['min_date', 'max_date'])

    # NB: Effectively ignoring different date types for now
    # Just grab the min and max dates across all types
    date_range['min_date'] = processed_dates.min(axis=1)
    date_range['max_date'] = processed_dates.max(axis=1)

    processed_dates = processed_dates.join(date_range)
    # processed_dates = processed_dates.map(
    #     lambda x: pd.to_datetime(x, format='%Y', errors='coerce'))

    df = pd.concat(
        [df.loc[:, :'Date'], processed_dates, df.loc[:, 'Language':]], axis=1)

    logging.info(f'No. of entries: {df_len}')
    logging.info(f'No. of question marked dates: {n_qd}')
    logging.info(f'No. of circa marked dates: {n_cd}')

    if debug:
        out_dir = file_path.parent.joinpath(file_path.stem + "_clean")
        out_dir.mkdir(parents=True, exist_ok=True)
        processed_dates.to_csv(_labelled_file(out_dir, file_path,
                                              'processed_dates'),
                               sep='\t')
        df.to_csv(_labelled_file(out_dir, file_path, 'cleaned_dates'),
                  sep='\t',
                  index=False)

    return df


def filter_nls_date(df: pd.DataFrame,
                    filter_date: int | None,
                    date_range: float,
                    folder: os.PathLike,
                    debug: bool
                    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Filter out dates within a range of years from 'filter_date'
    N.B. pandas' 'to_datetime()' can't deal with dates before 1678(!)
    so we're forcing all dates before then to 1678.

    :param df: The full dataframe of entries with min and max dates
    :param filter_date: date to filter, if 'None' then returns undated entries
    :param date_range: include dates +/- this value in years
    :param folder: folder to put filtered dataframe into for debug
    :param debug: if True save output to 'folder' as tsv
    :return filtered dataframe
    """

    mod_year = date_range + 0.1  # Add 0.1 to escape rounding errors
    if filter_date is not None:
        filter_label = str(filter_date)
        register_df = df.loc[((df['min_date'] - mod_year) < filter_date)
                             & ((df['max_date'] + mod_year) > filter_date)]
        register_df['min_date'] = register_df['min_date'].map(
            lambda d: 1678. if d < 1678. else d)
    else:
        filter_label = "undated"
        register_df = df.loc[df['min_date'].isnull() & df['max_date'].isnull()]

    if debug:
        register_path = Path(folder).parent.joinpath(
            folder.rstrip("/") + "_filtered_" + str(filter_label) + ".tsv")
        register_df.to_csv(register_path, sep='\t', index=False)

    logging.info(f"No. of entries filtered for date {filter_label} "
                 f": {len(register_df)}")
    return register_df.reindex()


def prepare_for_import(df: pd.DataFrame,
                       keep_columns: List[str],
                       source_library: str,
                       register_name: str) -> pd.DataFrame:
    """
    Insert and rename required columns to match promprint database schema

    Library codes:
        BODLEIAN_LIBRARY = "BDL"
        BRITISH_LIBRARY = "BTL"
        CAMBRIDGE_LIBRARY = "CAL"
        SCOTLAND_LIBRARY = "NLS"
        TRINITY_LIBRARY = "TCD"

    :param df: The dataframe to prepare
    :param keep_columns: Which columns to preserve (all others are discarded)
    :param source_library: The 3-letter library code for source library
    :param register_name: Name of register to which these entries are relevant
    :return df.DataFrame: Exportable dataframe
    """

    df_len = len(df)
    df.columns = df.columns.str.lower()
    df = df.loc[:, keep_columns]
    df = df.rename(columns={'creator': 'author'})

    # Don't need original index info, reset it to match new columns
    df = df.reset_index(drop=True)
    # Empty 'id' column required for django import for now
    df['id'] = pd.Series(np.nan, index=range(df_len))
    df['source_library'] = pd.Series([source_library] * df_len)
    df['register'] = pd.Series([register_name] * df_len)
    if register_name != "undated":
        df.loc[:,
               ["min_date", "max_date"
                ]] = df.loc[:, ["min_date", "max_date"]].map(
                    lambda x: pd.to_datetime(x, format='%Y', errors='coerce'))

    return df


def main(folder: str, debug: bool) -> None:

    file_paths = map(Path,
                     glob.glob(folder + '*.tsv') + glob.glob(folder + '*.txt'))
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
        df = (df.pipe(columnise_nls_data,
                      file_path=file_path,
                      debug=debug)
              .pipe(clean_nls_titles,
                    file_path=file_path,
                    debug=debug)
              .pipe(clean_nls_dates,
                    file_path=file_path,
                    debug=debug))
        compiled_df = pd.concat([compiled_df, df])

    print(f"Total No. of entries: {len(compiled_df)}")

    if debug:
        clean_path = Path(folder).parent.joinpath(
            folder.rstrip("/") + "_clean.tsv")
        compiled_df.to_csv(clean_path, sep='\t', index=False)

    for register_name, register_date in registers.items():
        register_df = filter_nls_date(compiled_df,
                                      register_date,
                                      date_range,
                                      folder,
                                      debug=debug)

        print(f"No. of entries after filtering for register {register_name}"
              f": {len(register_df)}")

        keep_columns = ['title', 'clean_title', 'creator',
                        'min_date', 'max_date']
        source_library = 'NLS'
        register_df = prepare_for_import(register_df,
                                         keep_columns,
                                         source_library,
                                         register_name)
        register_path = Path(folder).parent.joinpath(
            folder.rstrip("/") + "_filtered_" + register_name + "_db.tsv")
        register_df.to_csv(register_path, sep='\t', index=False)


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
