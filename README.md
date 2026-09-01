# Data wrangling scripts for the promprint project
These scripts are available for cleaning and organising different datasets gathered as part of the [Promiscuous Print ](https://www.promiscuousprint.com/) project.

The purpose of the scripts is to:

1. Make book titles more programmatically comparable across different collections of data. We can do this by 'cleaning' the title, creator, publisher and date columns e.g. by sensibly replacing non-alphanumeric characters and 'metadata'-like information (such as volume or edition number - see the [source](./src/lib/helpers.py) for more specifics)
2. Select and format the data so that it can be imported into later scripts or databases that are used to actually compare/match titles, and to view, edit and confirm or reject the matches that are found.

## Installation
These instructions assume you are using either linux or mac, but they should work for windows.

This repo uses a command line program called [`uv`](https://docs.astral.sh/uv/getting-started/installation/) to manage python versions and dependencies independently of your local python version and libraries. First use the previous link to install `uv`. Then [clone this repository](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) to your local machine.

Once you have a copy of this repo one your machine, navigate to the repository in the command line, from where you can run the code via include jupyter notebooks using `uv`:

```
cd path-to-repository/promprint-data-wrangling
uv run  --with jupyter jupyter lab
```

This should automatically open a browser tab at `http://localhost:8888/lab`, but if it doesn't you can manually enter that address into your browser to start using the jupyter server.

If you are using bash or a similar shell, you may wish to alias this by adding:
```
alias uvj="uv run --with jupyter jupyter lab"
```

to your `~/.bash_aliases` (or `~/.zsh` for mac), and then reloading your shell. Then you can simply use `uvj` when you want to start the jupyter server.

You can also run tests on the code:

```
uv run pytest                   # Run tests on the repository
```

Note that the first time you run any of the commands above, `uv` will automatically synchronise the libraries required to run these scripts.

## Datasets

So far there are two main datasets that are handled by these scripts, and two corresponding jupyter notebooks:

### The [National Library of Scotland](https://data.nls.uk/data/metadata-collections/catalogue-published-material/)'s full catalogue of published material:

This collection comprises all of the published material held by the National Library of Scotland (at the time of the database's compilation, i.e. it is not a live database). These can be downloaded in `xml` format, or `tsv` format. These scripts use the `tsv` format, which is essentially a tab-delimeted text table. The whole dataset is around 4GB large, and broken down into 50 separate files with around 100,000 entries per file (for a total of around 5 million entries).

To run the program on this dataset, you can open the `clean_nls.ipynb` in your running jupyter instance. This assumes a file structure as follows:

```
.
├── promprint-data
│   └── nls_catalog
└── promprint-data-wrangling
```

where `promprint-data-wrangling` is this repository location (the notebook is running from here), and `promprint-data/nls_catalog` contains the downloaded `tsv` files from the link above. Since there are multiple files in this catalog, we run the script on the folder containing the files, and not the individual files. You are free to rename either the `input_folder` if your data is elsewhere or the `output_folder` - the latter folder will be created for you, and the output data will be stored here alongside a timestamped `run_config.txt` file that contains info about the state of the repository and the arguments used in this run - this may help in future efforts to track changes and bugs in the algorithms used.

**Note that to date, the `tsv` file labelled number 45 in the NLS dataset is in a different/corrupted format to the rest of the dataset, and is not yet handled by these scripts.**

#### Date selection
The NLS catalog contains around 5 million entries with publication dates across multiple centuries. Our research is concerned with publications associated with specific years e.g. those published in `1863`, `1886` etc.

For efficient use in later processing, we can output separate, cleaned files containing titles published only in specific years. This is configured by editing the `src/config/nls.py` file. Here is an example configuration:

```
nls_config = {
    "registers": {"1863b": 1863, "1886b": 1886, "1907a|1907b": 1907, "undated": None},
    "date_range": 1.0,
}
```

The `nls_config` dictionary contains the following configuration variables, should you wish to change them:

`registers`: The values of this dictionary are the years that you wish to select for the output files. Each value will create a new file labelled using the key (inside quotes) e.g. `nls_catalog_1863b_export.tsv`. Additionaly the `"undated": None` pair will cause the script to output a separate file with only the NLS catalog entries that have either no date or a date that could not be parsed - the name of this output file will include `undated` e.g. `nls_catalog_undated_export.tsv`.


`date_range`: Each output file will contain titles published in the range of the year +/- `date_range`. In this example we will output a file containing NLS catalog entries for the years 1862, 1863 and 1864 (that is, 1863 +/- 1 year), as well as 1886 +/- 1 year etc. If we wanted to include 1861 and 1865, then we would change `date_range` to `2`.

### The Stationer's Hall copyright registers:
These collections comprise a written record of all books delivered to the Stationer's Hall under the copyright act during the dates covered by the register. The project titles each register according to the year it starts. If the year is not completely covered by the register it will have a suffix of `a` or `b`. `a` is used if the register starts at the beginning of the year but ends before it is over. `b` is used if the register does not start at the beginning of the year. Examples:

- "1907a" covers 9th Jan to 9th of Aug
- "1907b" covers 12th Aug to 31st Dec
- "1863b" covers 20th Feb 1863 to 29th Feb 1864. We do not have an "1863a"
- "1837" covers 3rd Jan to 30th Dec 1837

The data from each one of these collections is derived from separate pdf files for each register. These files contain photographic images of each page of that register. We extract textual data from these using the latest `gemini`, a multi-modal AI assistant. We access gemini via [Google AI Studio](https://aistudio.google.com/), and use custom prompts derived from those used by the [Archive Studio](https://github.com/mhumphries2323/Archive_Studio) project by Mark Humphries[^1], and which we will publish separately.

This process generates a `csv` (comma separated text) file which we can use with these scripts. If the `csv` file does not contain columns with the right headers, the script will complain with a list of headers that it is expecting.

TODO: Specify which columns are expected

Usage:

To run the program on this dataset, you can open the `clean_register.ipynb` in your running jupyter instance. This assumes a file structure as follows:

```
.
├── promprint-data
│   └── register_folder
│       └── register.csv
└── promprint-data-wrangling
```

where `promprint-data-wrangling` is this repository location (the notebook is running from here), and `promprint-data/register_folder/register.csv` contains the `csv` files created by the process described above. You are free to rename either the `input_file` if your data is elsewhere or the `output_folder` - the latter folder will be created for you, and the output data will be stored here alongside a timestamped `run_config.txt` file that contains info about the state of the repository and the arguments used in this run - this may help in future efforts to track changes and bugs in the algorithms used.


[^1]: Mark Humphries and Lianne C. Leddy, 2025. ArchiveStudio 1.0 Beta. Department of History: Wilfrid Laurier University.
