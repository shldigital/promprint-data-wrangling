import argparse
import numpy as np
import pandas as pd
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', help='Input file in tsv format')
    args = parser.parse_args()

    labels = [
        'Title', 'Creator', 'Type', 'Publisher', 'Date', 'Language', 'Format',
        'Relation', 'Rights', 'Identifier', 'Description', 'Subject',
        'Coverage', 'Contributor', 'Source'
    ]
    file_path = Path(args.filename)
    df = pd.read_csv(file_path, sep='\t', names=labels)

    df = df.map(lambda x: x.split(':')[1].rstrip('/').strip())
    df.to_csv(file_path.stem + '_columnar' + file_path.suffix,
              sep='\t',
              index=False)

    df_len = len(df)
    print(f'No of entries: {df_len}')

    # Separate out different types of date in case they're relevant
    dates_re = (r'(?:c(?:a\.?|irca|) ?(?P<circa_date>\d{2,4})|'
                r'(?P<question_date>\d{2,4})\?|'
                r'(?P<unqualified_date>\d{2,4}))')
    dates_df = df['Date'].str.extractall(dates_re).astype('float64')
    dates_df.to_csv(
        file_path.stem + '_dates' + file_path.suffix,
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
    processed_dates.to_csv(
        file_path.stem + '_processed_dates' + file_path.suffix,
        sep='\t',
    )

    df = pd.concat(
        [df.loc[:, :'Date'], processed_dates, df.loc[:, 'Language':]], axis=1)

    df.to_csv(file_path.stem + '_clean' + file_path.suffix,
              sep='\t',
              index=False)


if __name__ == "__main__":
    main()
