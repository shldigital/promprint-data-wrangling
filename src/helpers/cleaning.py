import re

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


