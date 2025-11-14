from src.lib.helpers import clean_title_string, remove_metadata, labelled_file
from pathlib import Path
from typing import List


def test_clean_title_string_lower_cases():
    input_string: str = "FRIENDS TO LOVERS"
    expected_string: str = "friends to lovers"
    output_string: str = clean_title_string(input_string)
    assert output_string == expected_string


def test_clean_title_string_strips_outer_whitespace():
    input_string: str = "\t\nkiller in shellview county \r"
    expected_string: str = "killer in shellview county"
    output_string: str = clean_title_string(input_string)
    assert output_string == expected_string


def test_clean_title_string_removes_apostrophes():
    input_string: str = "the lightkeeper's curse"
    expected_string: str = "the lightkeepers curse"
    output_string: str = clean_title_string(input_string)
    assert output_string == expected_string


def test_clean_title_string_removes_backticks():
    input_string: str = "the lightkeeper`s curse"
    expected_string: str = "the lightkeepers curse"
    output_string: str = clean_title_string(input_string)
    assert output_string == expected_string


def test_clean_title_string_replaces_seq_of_other_chars_with_single_space():
    input_string: str = "aÆ[date]/with/''\"\"£$%^*()-+_={}@~#!<>,?.death"
    expected_string: str = "a date with death"
    output_string: str = clean_title_string(input_string)
    assert output_string == expected_string


def test_clean_title_string_only_single_spaces():
    input_string: str = "hiding  in  alaska"
    expected_string: str = "hiding in alaska"
    output_string: str = clean_title_string(input_string)
    assert output_string == expected_string


def test_clean_title_string_replaces_ampersand_string():
    input_string: str = "mills &amp; boon"
    expected_string: str = "mills and boon"
    output_string: str = clean_title_string(input_string)
    assert output_string == expected_string


def test_clean_title_string_replaces_ampersand_character():
    input_string: str = "mills & boon"
    expected_string: str = "mills and boon"
    output_string: str = clean_title_string(input_string)
    assert output_string == expected_string


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
    input_strings: List[str] = ["second chance [microform]",
                                "second chance [illustrated]",
                                "second chance [a novel]",
                                "second chance [plates]"]
    expected_strings: List[str] = ["second chance"] * 4
    output_strings: List[str] = map(remove_metadata, input_strings)
    assert list(output_strings) == expected_strings


def test_remove_metadata_removes_volume_edition_metadata():
    input_strings: List[str] = ["just my luck n 23",
                                "just my luck ed 34",
                                "just my luck vol 93",
                                "just my luck vols 190-321",
                                "just my luck volume 38",
                                "just my luck volumes 23 - 34"]
    expected_strings: List[str] = ["just my luck"] * 6
    output_strings: List[str] = map(remove_metadata, input_strings)
    assert list(output_strings) == expected_strings


def test_labelled_file_changes_ext():
    input_filename: str = "./tests/test_register/test_register.csv"
    input_path: Path = Path(input_filename)
    out_dir = "./tests/test_register/"
    out_path: Path = Path(out_dir)
    expected_name: str = "./tests/test_register/test_register_labelled.tsv"
    expected_path: Path = Path(expected_name)
    assert labelled_file(out_path, input_path, "labelled", suffix=".tsv") == expected_path


def test_labelled_file_doesnt_change_ext():
    input_filename: str = "./tests/test_register/test_register.csv"
    input_path: Path = Path(input_filename)
    out_dir = "./tests/test_register/"
    out_path: Path = Path(out_dir)
    expected_name: str = "./tests/test_register/test_register_labelled.csv"
    expected_path: Path = Path(expected_name)
    assert labelled_file(out_path, input_path, "labelled") == expected_path
