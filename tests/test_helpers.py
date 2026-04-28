import pandas as pd
import pytest

from src.lib.helpers import (
    clean_text,
    remove_metadata,
    labelled_file,
    format_library_set,
)
from pathlib import Path
from typing import List


def test_clean_text_lower_cases():
    input_string: str = "FRIENDS TO LOVERS"
    expected_string: str = "friends to lovers"
    output_string: str = clean_text(input_string)
    assert output_string == expected_string


def test_clean_text_strips_outer_whitespace():
    input_string: str = "\t\nkiller in shellview county \r"
    expected_string: str = "killer in shellview county"
    output_string: str = clean_text(input_string)
    assert output_string == expected_string


def test_clean_text_removes_apostrophes():
    input_string: str = "the lightkeeper's curse"
    expected_string: str = "the lightkeepers curse"
    output_string: str = clean_text(input_string)
    assert output_string == expected_string


def test_clean_text_removes_backticks():
    input_string: str = "the lightkeeper`s curse"
    expected_string: str = "the lightkeepers curse"
    output_string: str = clean_text(input_string)
    assert output_string == expected_string


def test_clean_text_replaces_seq_of_other_chars_with_single_space():
    input_string: str = "aÆ[date]/with/''\"\"£$%^*()-+_={}@~#!<>,?.death"
    expected_string: str = "a date with death"
    output_string: str = clean_text(input_string)
    assert output_string == expected_string


def test_clean_text_only_single_spaces():
    input_string: str = "hiding  in  alaska"
    expected_string: str = "hiding in alaska"
    output_string: str = clean_text(input_string)
    assert output_string == expected_string


def test_clean_text_replaces_ampersand_string():
    input_string: str = "mills &amp; boon"
    expected_string: str = "mills and boon"
    output_string: str = clean_text(input_string)
    assert output_string == expected_string


def test_clean_text_replaces_ampersand_character():
    input_string: str = "mills & boon"
    expected_string: str = "mills and boon"
    output_string: str = clean_text(input_string)
    assert output_string == expected_string


def test_clean_text_handles_blank_entry():
    input_string: str = ""
    expected_string: str = ""
    output_string: str = clean_text(input_string)
    assert output_string == expected_string


def test_clean_text_handles_etcetera():
    input_strings: List[str] = [
        "&c",
        "et cetera",
        "etcetera",
    ]
    expected_strings: List[str] = ["etc"] * 3
    output_strings: List[str] = map(clean_text, input_strings)
    assert list(output_strings) == expected_strings


def test_remove_metadata_lower_cases():
    input_string: str = "FRIENDS TO LOVERS"
    expected_string: str = "friends to lovers"
    output_string: str = remove_metadata(input_string)
    assert output_string == expected_string


def test_remove_metadata_only_single_spaces():
    input_string: str = "hiding  in  alaska"
    expected_string: str = "hiding in alaska"
    output_string: str = remove_metadata(input_string)
    assert output_string == expected_string


def test_remove_metadata_strips_outer_whitespace():
    input_string: str = "\t\nkiller in shellview county \r"
    expected_string: str = "killer in shellview county"
    output_string: str = remove_metadata(input_string)
    assert output_string == expected_string


def test_remove_metadata_removes_square_bracket_metadata():
    input_strings: List[str] = [
        "second chance [microform]",
        "second chance [illustrated]",
        "second chance [a novel]",
        "second chance [plates]",
    ]
    expected_strings: List[str] = ["second chance"] * 4
    output_strings: List[str] = map(remove_metadata, input_strings)
    assert list(output_strings) == expected_strings


def test_remove_metadata_removes_volume_edition_metadata():
    input_strings: List[str] = [
        "just my luck n 23",
        "just my luck ed 34",
        "just my luck vol 93",
        "just my luck vols 190-321",
        "just my luck volume 38",
        "just my luck volumes 23 - 34",
        "just my luck pt 9",
    ]
    expected_strings: List[str] = ["just my luck"] * 7
    output_strings: List[str] = map(remove_metadata, input_strings)
    assert list(output_strings) == expected_strings


def test_labelled_file_changes_ext():
    input_filename: str = "./tests/test_register/test_register.csv"
    input_path: Path = Path(input_filename)
    out_dir = "./tests/test_register/"
    out_path: Path = Path(out_dir)
    expected_name: str = "./tests/test_register/test_register_labelled.tsv"
    expected_path: Path = Path(expected_name)
    assert (
        labelled_file(out_path, input_path, "labelled", suffix=".tsv") == expected_path
    )


def test_labelled_file_doesnt_change_ext():
    input_filename: str = "./tests/test_register/test_register.csv"
    input_path: Path = Path(input_filename)
    out_dir = "./tests/test_register/"
    out_path: Path = Path(out_dir)
    expected_name: str = "./tests/test_register/test_register_labelled.csv"
    expected_path: Path = Path(expected_name)
    assert labelled_file(out_path, input_path, "labelled") == expected_path


def test_new_index_added_to_formatted_library_set():
    source_library = "NLS"
    df = pd.read_csv(
        "./tests/test_files/nls_sample_filtered_1863b.tsv", sep="\t", index_col=0
    )
    original_index = df.index
    new_df = format_library_set(df, None, source_library, "1863b")
    updated = map(
        lambda x, y: x == f"{source_library}:{y}", new_df.index, original_index
    )
    assert all(updated)


def test_duplicate_indices_raises():
    source_library = "NLS"
    df = pd.read_csv(
        "./tests/test_files/nls_duplicate_indices.tsv", sep="\t", index_col=0
    )
    with pytest.raises(IndexError):
        format_library_set(df, None, source_library, "1863b")


test_edition_strings = [
    ("Mechanic's Magazine Pt 13. Vol. 8. Pts 1 to 9", "mechanics magazine"),
    ("Mechanic's Magazine pt 13. vol. 8. pts 1 to 9", "mechanics magazine"),
    ("Mechanic's Magazine part 13. vol. 8. parts 1 to 9", "mechanics magazine"),
    ("Mechanic's Magazine pt XIII. vol. VIII. pts I to IX", "mechanics magazine"),
    ("English Womens Domestic Mag. no 19 to 25-", "english womens domestic mag"),
    ("English Womens Domestic Mag. no XIX to XXV-", "english womens domestic mag"),
    ("Beeton's Dictionary nos. 23-25. & 35 to 37.", "beetons dictionary"),
    ("Beeton's Dictionary nos. XXII-XXV. & XXXV to XXXVII.", "beetons dictionary"),
    ("Beeton's Dictionary nos. 23 & 35", "beetons dictionary"),
    ("Beeton's Dictionary nos. 23 & 35 to 37.", "beetons dictionary"),
    ("Beeton's Dictionary numbers. 23 & 35 to 37.", "beetons dictionary"),
    ("Social Science Review no. 38 vol 2", "social science review"),
    ("Social Science Review no. XXXVIII vol II", "social science review"),
    ("Social Science Review number XXXVIII vol II", "social science review"),
    ("Hair drepers Journal March. 1863.", "hair drepers journal"),
    ("Hair drepers Journal March. MDCCCLXIII.", "hair drepers journal"),
    ("Hair drepers Journal 4th edition", "hair drepers journal"),
    ("Hair drepers Journal March edition", "hair drepers journal"),
    ("Hair drepers Journal March ed.", "hair drepers journal"),
    ("just my luck n 23", "just my luck"),
    ("just my luck vols 190-321", "just my luck"),
    ("just my luck volumes 23 - 34", "just my luck"),
    ("just my luck editions 23 - 34", "just my luck"),
    ("just my luck pt 9", "just my luck"),
    ("just my luck ed. 9", "just my luck"),
    ("just my luck edition 9", "just my luck"),
    ("just my luck n XXIII", "just my luck"),
    ("just my luck vols XCX-CCCXXI", "just my luck"),
    ("just my luck no. 4", "just my luck"),
    (
        "a series of Essays 2 Vols No. 214.",
        "a series of essays",
    ),
]


@pytest.mark.parametrize("raw_string, cleaned_string", test_edition_strings)
def test_remove_edition_data(raw_string, cleaned_string):
    no_metadata = remove_metadata(raw_string)
    result = clean_text(no_metadata)
    assert result == cleaned_string
