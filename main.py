import argparse
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

    dates_re = (r'(?:c(?:a\.?|irca|) ?(?P<circa_date>\d{2,4})|'
                r'(?P<unqualified_date>\d{2,4}))')
    dates_df = df['Date'].str.extractall(dates_re).astype('Int32')

    # TODO: Check with team if this is a good idea
    dates_df = dates_df.map(lambda x: x + 1900 if x < 100 else x)

    max_dates_df = dates_df.groupby(level=0).max()
    max_circa_dates = max_dates_df['circa_date']
    max_unqualified_dates = max_dates_df['unqualified_date']

    min_dates_df = dates_df.groupby(level=0).min()
    min_circa_dates = min_dates_df['circa_date']
    min_unqualified_dates = min_dates_df['unqualified_date']

    print(max_circa_dates[23])
    print(min_circa_dates[23])
    print(max_unqualified_dates[23])
    print(max_unqualified_dates[516])
    print(min_unqualified_dates[516])

    dates_df.to_csv(
        file_path.stem + '_dates' + file_path.suffix,
        sep='\t',
    )

    df.to_csv(file_path.stem + '_clean' + file_path.suffix,
              sep='\t',
              index=False)


if __name__ == "__main__":
    main()
