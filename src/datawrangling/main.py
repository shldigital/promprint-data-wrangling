import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lib'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'cli'))
from cli import clean_nls
from cli import clean_register

logger = logging.getLogger('')
logging.basicConfig(level=logging.INFO,
                    filename="promprint-data-wrangling.log",
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filemode='w')

console = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)


def main():
    parser = argparse.ArgumentParser(
        description="General scripts for cleaning promprint data")
    subparsers = parser.add_subparsers(help="Datasets to manipulate")
    parser.add_argument("-d", "--debug",
                        action='store_true',
                        help='Save intermediate stages of cleaning to file')

    nls_parser = subparsers.add_parser('nls',
                                       help="National Library of Scotland data")
    nls_parser.add_argument('input_folder',
                            type=str,
                            help='Folder of input files in txt format')
    nls_parser.add_argument('output_folder',
                            type=str,
                            help="folder for the formatted output")
    nls_parser.add_argument('config_file',
                            type=str,
                            help='config file with registers to filter')
    nls_parser.set_defaults(func=clean_nls.main)

    register_parser = subparsers.add_parser('register',
                                            help="Data from the Stationer's Hall registers")
    register_parser.add_argument('input_file',
                                 type=str,
                                 help="register file in csv format")
    register_parser.add_argument('output_folder',
                                 type=str,
                                 help="folder for the formatted output")
    register_parser.set_defaults(func=clean_register.main)

    args = parser.parse_args()

    if args.debug:
        console.setLevel(logging.INFO)
    else:
        console.setLevel(logging.WARNING)
    logging.getLogger('').addHandler(console)

    args.func(**vars(args))


if __name__ == "__main__":
    main()
