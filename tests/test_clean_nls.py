import pathlib

from src.cli.clean_nls import main

input_folder = "./tests/test_files/test_nls/"
dated_filename = "test_nls_1863b_export.tsv"


def test_returns_tsv_file(tmp_path):
    main(input_folder, tmp_path, False)
    assert pathlib.Path.exists(tmp_path / dated_filename)
