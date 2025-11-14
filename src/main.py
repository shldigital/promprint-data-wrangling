import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lib'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'cli'))
import helpers
import nls
from cli import clean_nls

logger = logging.getLogger('')
logging.basicConfig(level=logging.INFO,
                    filename="promprint-cleaning.log",
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filemode='w')

console = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)


def main():
    parser = argparse.ArgumentParser(
        description="General scripts for cleaning promprint data at various stages")
    subparsers = parser.add_subparsers(help="Datasets to manipulate")
    parser.add_argument("-d", "--debug",
                        action='store_true',
                        help='Save intermediate stages of cleaning to file')

    nls_parser = subparsers.add_parser('nls')
    nls_parser.add_argument('folder',
                            type=str,
                            help='Folder of input files in tsv format')
    nls_parser.set_defaults(func=clean_nls.main)

    register_parser = subparsers.add_parser("register")
    register_parser.add_argument('file',
                                 type=str,
                                 help="File to clean, in csv format")
    args = parser.parse_args()

    if args.debug:
        console.setLevel(logging.INFO)
    else:
        console.setLevel(logging.WARNING)
    logging.getLogger('').addHandler(console)

    args.func(**vars(args))


if __name__ == "__main__":
    main()
