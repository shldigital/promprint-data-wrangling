import ast
import glob

from src.cli.clean_nls import main

input_folder = "./tests/test_files/test_nls/"
config_file = "./tests/test_files/test_config.py"


def test_returns_tsv_file(tmp_path):
    main(input_folder, tmp_path, config_file, False)
    outputs = glob.glob(str(tmp_path) + '/*.tsv')
    assert len(outputs) > 0


def test_config_names_outputs(tmp_path):
    main(input_folder, tmp_path, config_file, False)
    outputs = glob.glob(str(tmp_path) + '/*.tsv')
    with open(config_file) as data:
        config = ast.literal_eval(data.read())
    registers = list(config["NLS"]["registers"].keys())
    present = []
    for output in outputs:
        present.append(any([output.find(register) for register in registers]))
    assert all(present)

