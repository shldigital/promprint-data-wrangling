import numpy as np
import pandas as pd
import re
from typing import List

from pathlib import PosixPath


def labelled_file(out_dir: PosixPath, file_path: PosixPath,
                  label: str) -> PosixPath:
    """
    Insert a text label into a filename and append to directory
    """
    new_name = file_path.stem + '_' + label + '.tsv'
    return out_dir / new_name


def remove_metadata(title_string: str) -> str:
    """
    Remove strings and numbers not directly related to the title of the entry
    """
    square_brackets_clean = re.sub(
        r'\[(?:microform|illustrated|a novel|plates)\]', '',
        title_string.lower())
    editions_clean = re.sub(r'\b(?:n|ed|vol(?:s|ume|umes|))\b', '',
                            square_brackets_clean)
    return re.sub(r'\d{1,4}', '', editions_clean)


def clean_title_string(title_string: str) -> str:
    """
    Remove/replace ampersands, apostrophes and multi-spaces
    """
    no_ampersand = re.sub(r'(&amp;|&)', 'and', title_string)
    no_apostrophe = re.sub(r"['`]", '', no_ampersand)
    alphanum = re.sub(r'[^a-zA-Z0-9]', ' ', no_apostrophe)
    single_spaced = re.sub(r'\s{2,}', ' ', alphanum)
    return single_spaced.strip().lower()


def clean_titles(df: pd.DataFrame, file_path: PosixPath,
                 debug: bool) -> pd.DataFrame:
    """
    Collecting the different title cleaning functions here

    :param df: The dataframe with uncleaned titles in columnar format
    :param file_path: File path of original data, to name debug output files
    :param debug: if True then save the dataframe out as a tsv file
    :return pd.DataFrame: The columnar dataframe
    """
    clean_titles = (
        df['Title'].map(remove_metadata)
        .map(clean_title_string)
        .rename('clean_title')
    )

    df = pd.concat(
        [df.loc[:, :'Title'], clean_titles, df.loc[:, 'Creator':]], axis=1)
    if debug:
        out_dir = file_path.parent.joinpath(file_path.stem + "_clean")
        out_dir.mkdir(parents=True, exist_ok=True)
        df.to_csv(labelled_file(out_dir, file_path, 'clean_titles'),
                  sep='\t',
                  index=False)
    return df


def format_library_set(df: pd.DataFrame,
                       drop_columns: List[str] | None,
                       source_library: str,
                       register_name: str) -> pd.DataFrame:
    """
    Format a library dataset to match promprint database schema

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
