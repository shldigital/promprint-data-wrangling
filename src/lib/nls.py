import lib.helpers as helpers
import logging
import numpy as np
import pandas as pd
import re

from pathlib import Path

logger = logging.getLogger('')


def columnise_nls_data(df: pd.DataFrame, file_path: Path,
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
        'title', 'creator', 'type', 'publisher', 'date', 'language', 'format',
        'relation', 'rights', 'identifier', 'description', 'subject',
        'coverage', 'contributor', 'source'
    ]
    df.columns = labels
    if debug:
        out_dir = file_path.parent.joinpath(file_path.stem + "_clean")
        out_dir.mkdir(parents=True, exist_ok=True)
        df.to_csv(helpers.labelled_file(out_dir, file_path,
                                        'columnar', ".tsv"),
                  sep='\t')
    return df


def add_file_data_to_index(df: pd.DataFrame, file_path: Path):
    """
    Adds a file identifier to the data index (which is the row number).
    Expects a file path like '/path/to/filename_filenumber.txt'
    and adds "filenumber" as a prefix to the index

    e.g. '/path/to/data_34.txt' outputs '34:<row_number>' as the index

    If the file_path does not follow this format then the full filename will
    be taken as the prefix

    :param df: column formatted dataframe of NLS data
    :type df: pd.DataFrame
    :param file_path: Path to the file that the dataframe was loaded from
    :type file_path: pathlib.Path
    :return: Dataframe with "file_id" column added
    :rtype: pd.DataFrame
    """
    try:
        prefix = re.search(r"(\d{2})\.txt", str(file_path)).group(1)
    except AttributeError:
        logger.warning(f"{file_path} not numbered, using full path as index prefix")
        prefix = file_path.stem
    df.index = df.index.map(lambda x: f'{prefix}:{x}')
    return df


def clean_nls_dates(df: pd.DataFrame, file_path: Path,
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

    # Separate out different types of date in case they're relevant
    dates_re = (r'(?:c(?:a\.?|irca|) ?(?P<circa_date>\d{4})|'
                r'(?P<question_date>\d{4})\?|'
                r'(?P<unqualified_date>\d{4}))')

    # create a dataframe of date matches with original index and match index
    # and match index as a multi-index
    dates_df = df['date'].str.extractall(dates_re).astype('float64')
    if debug:
        out_dir = file_path.parent.joinpath(file_path.stem + "_clean")
        out_dir.mkdir(parents=True, exist_ok=True)
        dates_df.to_csv(
            helpers.labelled_file(out_dir, file_path, 'datetypes', ".tsv"),
            sep='\t',
        )

    # Grab the question and circa dates with their original indices
    question_dates: pd.Series = dates_df.pop('question_date').groupby(
        level=0).first().dropna()

    circa_dates: pd.Series = dates_df.pop('circa_date').groupby(level=0).first().dropna()

    # Grab the unqualified dates with original indices into one series,
    # if there are more than one, take the lowest
    min_uq_dates: pd.Series = dates_df.groupby(level=0).min().rename(
        columns={
            'unqualified_date': 'min_uq_date'
        }).dropna()

    # Grab the unqualified dates with original indices into one series,
    # if there are more than one, take the highest
    max_uq_dates: pd.Series = dates_df.groupby(level=0).max().rename(
        columns={
            'unqualified_date': 'max_uq_date'
        }).dropna()

    # Make a new empty dataframe to hold the sorted dates data
    # the new frame has the same size/index as the original
    processed_dates = pd.DataFrame(
        np.nan,
        index=df.index,
        columns=['question_date', 'circa_date', 'min_uq_date', 'max_uq_date'])

    # Insert the various dates at their labelled indices
    processed_dates.update(question_dates)
    processed_dates.update(circa_dates)
    processed_dates.update(min_uq_dates['min_uq_date'])
    processed_dates.update(max_uq_dates['max_uq_date'])

    # Make a new empty dataframe to hold the reduced dates data
    # the new frame has the same size/index as the original
    date_range = pd.DataFrame(np.nan,
                              index=df.index,
                              columns=['min_date', 'max_date'])

    # NB: Effectively ignoring different date types for now
    # Just grab the min and max dates across all types
    date_range['min_date'] = processed_dates.min(axis=1)
    date_range['max_date'] = processed_dates.max(axis=1)

    df = pd.concat(  # type: ignore[call-overload]
        [df.loc[:, :'date'], date_range, df.loc[:, 'language':]], axis=1)  # type: ignore[misc]

    if debug:
        out_dir: Path = file_path.parent.joinpath(file_path.stem + "_clean")
        out_dir.mkdir(parents=True, exist_ok=True)
        processed_dates.to_csv(helpers.labelled_file(out_dir, file_path,
                                                     'processed_dates',
                                                     ".tsv"),
                               sep='\t')
        df.to_csv(helpers.labelled_file(out_dir, file_path, 'cleaned_dates',
                                        ".tsv"),
                  sep='\t')

    return df


def filter_nls_date(df: pd.DataFrame,
                    filter_date: int | None,
                    date_range: float,
                    ) -> pd.DataFrame:
    """
    Filter out dates within a range of years from 'filter_date'
    N.B. pandas' 'to_datetime()' can't deal with dates before 1678(!)
    so we're forcing all dates before then to 1678.

    :param df: The full dataframe of entries with min and max dates
    :param filter_date: date to filter, if 'None' then returns undated entries
    :param date_range: include dates +/- this value in years
    :return filtered dataframe
    """

    mod_year: float = date_range + 0.1  # Add 0.1 to escape rounding errors
    if filter_date is not None:
        register_df = df.loc[((df['min_date'] - mod_year) < filter_date)
                             & ((df['max_date'] + mod_year) > filter_date)]
        # Oh hey! A horrible hack! Apparently datetime64[ns] format has a
        # problem with dates before 1678
        register_df.loc[:, 'min_date'] = register_df.loc[:, 'min_date'].map(
            lambda d: 1678. if d < 1678. else d)
    else:
        register_df = df.loc[df['min_date'].isnull() & df['max_date'].isnull()]

    return register_df.reindex()
