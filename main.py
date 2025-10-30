import argparse
from functools import partial
import glob
import numpy as np
import os
import pandas as pd
from pathlib import Path

DEBUG = False


def labelled_file(out_dir: os.PathLike, file_path: os.PathLike,
                  label: str) -> os.PathLike:
    new_name = file_path.stem + '_' + label + file_path.suffix
    return out_dir / new_name


def clean_dataframe(df: pd.DataFrame, file_path: os.PathLike,
                    register_date: float) -> tuple[pd.DataFrame, pd.DataFrame]:

    out_dir = file_path.parent.joinpath(file_path.stem + "_clean")
    out_dir.mkdir(parents=True, exist_ok=True)

    df = df.map(lambda x: x.split(':')[1].rstrip('/').strip())
    if DEBUG:
        df.to_csv(labelled_file(out_dir, file_path, 'columnar'),
                  sep='\t',
                  index=False)

    df_len = len(df)
    print(f'No. of entries: {df_len}')

    # Separate out different types of date in case they're relevant
    dates_re = (r'(?:c(?:a\.?|irca|) ?(?P<circa_date>\d{4})|'
                r'(?P<question_date>\d{4})\?|'
                r'(?P<unqualified_date>\d{4}))')
    dates_df = df['Date'].str.extractall(dates_re).astype('float64')
    if DEBUG:
        dates_df.to_csv(
            labelled_file(out_dir, file_path, 'dates'),
            sep='\t',
        )

    question_dates = dates_df.pop('question_date').groupby(
        level=0).first().dropna()
    n_qd = len(question_dates)
    print(f'No. of question marked dates: {n_qd}')

    circa_dates = dates_df.pop('circa_date').groupby(level=0).first().dropna()
    n_cd = len(circa_dates)
    print(f'No. of circa marked dates: {n_cd}')

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

    if DEBUG:
        processed_dates.to_csv(
            labelled_file(out_dir, file_path, 'processed_dates'),
            sep='\t',
        )

    df = pd.concat(
        [df.loc[:, :'Date'], processed_dates, df.loc[:, 'Language':]], axis=1)

    df.to_csv(labelled_file(out_dir, file_path, 'clean'),
              sep='\t',
              index=False)

    # TODO: split function here
    register_date = 1863
    register_df = df.loc[((df['min_date'] - 1.1) < register_date)
                         & ((df['max_date'] + 1.1) > register_date)]
    register_df.to_csv(labelled_file(out_dir, file_path,
                                     'filtered_' + str(register_date)),
                       sep='\t',
                       index=False)

    missing_df = df.loc[df['min_date'].isnull()]
    missing_df.to_csv(labelled_file(out_dir, file_path, 'missing'),
                      sep='\t',
                      index=False)

    n_exact = len(df.loc[((df['min_date'] - 0.9) < register_date)
                         & ((df['max_date'] + 0.9) > register_date)])
    n_extended = len(register_df)
    n_missing = len(missing_df)
    print(f'No. of missing/unrecognised dates: {n_missing}')
    print(f"No. of entries filtered for date {register_date} "
          f"(exact, extended): {n_exact, n_extended}")
    return register_df, missing_df


def main(folder: str) -> None:
    labels = [
        'Title', 'Creator', 'Type', 'Publisher', 'Date', 'Language', 'Format',
        'Relation', 'Rights', 'Identifier', 'Description', 'Subject',
        'Coverage', 'Contributor', 'Source'
    ]

    file_paths = map(Path,
                     glob.glob(folder + '*.tsv') + glob.glob(folder + '*.txt'))
    register_date = 1863
    register_df = pd.DataFrame()
    missing_df = pd.DataFrame()
    for file_path in file_paths:
        print(file_path)
        df = pd.read_csv(file_path,
                         sep='\t',
                         names=labels,
                         engine='python',
                         on_bad_lines=partial(lambda line: line[:15]))
        try:
            new_register_df, new_missing_df = clean_dataframe(
                df, file_path, register_date)
            register_df = pd.concat([register_df, new_register_df])
            missing_df = pd.concat([missing_df, new_missing_df])
        except Exception as e:
            print(f"Exception while processing {file_path},\n{e}")

    register_df.loc[:, ["min_date", "max_date"]] = register_df.loc[:, [
        "min_date", "max_date"
    ]].map(lambda x: pd.to_datetime(x, format='%Y', errors='coerce'))

    register_path = Path(folder).parent.joinpath(
        folder.rstrip("/") + "_filtered_" + str(register_date) + ".tsv")
    register_df.to_csv(register_path, sep='\t', index=False)

    missing_path = Path(folder).parent.joinpath(
        folder.rstrip("/") + "_missing.tsv")
    missing_df.to_csv(missing_path, sep='\t', index=False)

    print(f"No. of entries after filtering (extended): {len(register_df)}")
    print(f"No. of entries with no date: {len(missing_df)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('folder', help='Folder of input files in tsv format')
    args = parser.parse_args()

    main(args.folder)
