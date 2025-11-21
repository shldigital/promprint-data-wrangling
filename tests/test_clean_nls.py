import glob

from src.cli.clean_nls import main

input_folder = "./tests/test_files/test_nls/"


def test_returns_tsv_file(tmp_path):
    main(input_folder, tmp_path, False)
    outputs = glob.glob(str(tmp_path) + '/*.tsv')
    assert len(outputs) > 0
