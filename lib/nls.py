import numpy as np
import pandas as pd
import helpers

from pathlib import Path, PosixPath
from typing import List


def columnise_nls_data(df: pd.DataFrame, file_path: PosixPath,
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
        df.to_csv(helpers.labelled_file(out_dir, file_path, 'columnar'),
                  sep='\t',
                  index=False)
    return df


def clean_nls_dates(df: pd.DataFrame, file_path: PosixPath,
                    debug: bool) -> pd.DataFrame:
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
            helpers.labelled_file(out_dir, file_path, 'datetypes'),
            sep='\t',
        )

    # Grab the question and circa dates with their original indices
    question_dates = dates_df.pop('question_date').groupby(
        level=0).first().dropna()

    circa_dates = dates_df.pop('circa_date').groupby(level=0).first().dropna()

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

    df = pd.concat(  # type: ignore[call-overload]
        [df.loc[:, :'Date'], date_range, df.loc[:, 'Language':]], axis=1)  # type: ignore[misc]

    if debug:
        out_dir = file_path.parent.joinpath(file_path.stem + "_clean")
        out_dir.mkdir(parents=True, exist_ok=True)
        processed_dates.to_csv(helpers.labelled_file(out_dir, file_path,
                                                     'processed_dates'),
                               sep='\t')
        df.to_csv(helpers.labelled_file(out_dir, file_path, 'cleaned_dates'),
                  sep='\t',
                  index=False)

    return df


def filter_nls_date(df: pd.DataFrame,
                    filter_date: int | None,
                    date_range: float,
                    folder: str,
                    debug: bool
                    ) -> pd.DataFrame:
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
        register_df.loc[:, 'min_date'] = register_df.loc[:, 'min_date'].map(
            lambda d: 1678. if d < 1678. else d)
    else:
        filter_label = "undated"
        register_df = df.loc[df['min_date'].isnull() & df['max_date'].isnull()]

    if debug:
        register_path = Path(folder).parent.joinpath(
            folder.rstrip("/") + "_filtered_" + str(filter_label) + ".tsv")
        register_df.to_csv(register_path, sep='\t', index=False)

    return register_df.reindex()


def prepare_for_import(df: pd.DataFrame,
                       drop_columns: List[str] | None,
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
    :param drop_columns: Which columns to drop before export
    :param source_library: The 3-letter library code for source library
    :param register_name: Name of register to which these entries are relevant
    :return df.DataFrame: Exportable dataframe
    """

    df_len = len(df)
    df.columns = df.columns.str.lower()
    if drop_columns is not None:
        df = df.drop(columns=drop_columns)

    # Don't need original index info, reset it to match new columns
    df = df.reset_index(drop=True)
    # Empty 'id' column required for django import for now
    df['id'] = pd.Series(np.nan, index=range(df_len))
    df['source_library'] = pd.Series([source_library] * df_len)
    df['register'] = pd.Series([register_name] * df_len)
    if register_name != "undated":
        df[["min_date", "max_date"]] = df[["min_date", "max_date"]].astype(object)
        df.loc[:,
               ["min_date", "max_date"
                ]] = df.loc[:, ["min_date", "max_date"]].map(
                    lambda x: pd.to_datetime(x, format='%Y', errors='coerce'))
        df[["min_date", "max_date"]] = df[["min_date", "max_date"]].astype(
           'datetime64[ns]')

    return df
