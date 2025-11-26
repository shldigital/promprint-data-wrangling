# Data wrangling scripts for the promprint project
These scripts are available for cleaning and organising different datasets gathered as part of the [Promiscuous Print ](https://www.promiscuousprint.com/) project.

The purpose of the scripts is to:

1. Make book titles more programmatically comparable across different collections of data. We can do this by 'cleaning' titles i.e. by sensibly replacing non-alphanumeric characters and 'metadata'-like information (such as volume or edition number, see the [source](./src/lib/helpers.py:L19) for more specifics)
2. Format the data so that it can be imported into later scripts or databases that are used to actually compare/match titles, and to view, edit and confirm or reject the matches that are found.

## Installation
This repo uses a command line program called [`uv`](https://docs.astral.sh/uv/getting-started/installation/) to manage python versions and dependencies independently of your local python version and libraries. First use the link above to install `uv`. Then [clone this repository](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) to your local machine.

Once you have a copy of this repo one your machine, navigate to the folder in the command line, from where you can run the scripts using `uv`:

```
uv run src/main.py -h           # Help for the main cli script
uv run src/main.py nls -h       # Help for the National Library of Scotland data script
uv run src/main.py register -h  # Help for the Stationer's Hall data script

uv run pytest                   # Run function tests
```

Note that the first time you run any of the commands above, `uv` will automatically synchronise the libraries required to run these scripts.

## Datasets

So far there are two main datasets that are handled by these scripts:

### The [National Library of Scotland](https://data.nls.uk/data/metadata-collections/catalogue-published-material/
)'s full catalogue of published material:

This collection comprises all of the published material held by the National Library of Scotland (at the time of the database's compilation, i.e. it is not a live database). These can be downloaded in `xml` format, or `txt` format. These scripts use the `txt` format, which is essentially a tab-delimeted text table. The whole dataset is around 4GB large, and broken down into 50 separate files with around 100,000 entries per file (for a total of around 5 million entries).

Since there are multiple files in this catalog, we run the script on the folder containing the files.

Usage:
```
uv run src/main.py nls /folder/of/nls/data/ /output/folder/ ./config.py
```

**Note that to date, the file labelled number 45 is in a different format to the rest of the dataset, and is not yet handled by these scripts. This file must therefore be removed from the folder in order to succesfully process the rest.****

The output goes into a separate folder of the user's choice. The user also passes in a config file, here named `config.py`, but which can take any name. This file contains a python dictionary in the following format:

```
{
    "NLS": {
        "registers": {
            "1863b": 1863,
            "undated": None
        },
        "date_range": 1.
    }
}
```

Here, the `registers` dictionary holds a list of register names (inside quotes) and the year of that register. The year is used, along with the `date_range` value, to filter out entries in the NLS catalog. In this example we will output a file containing only NLS catalog entries for the years 1862, 1863 and 1864 (that is, 1863 +/- 1 year). If we wanted to include 1861 and 1865, then we would change `date_range` to `2`. The name of the output file will include the register name defined in the dictionary e.g. `nls_catalog_1863b_export.tsv`. Additionaly the `"undated": None` pair will cause the script to output a separate file with only the NLS catalog entries that have either no date or a date that could not be parsed - the name of this output file will include `undated` e.g. `nls_catalog_undated_export.tsv`.

### The Stationer's Hall copyright registers:
These collections comprise a written record of all books delivered to the Stationer's Hall under the copyright act during the dates covered by the register. The project titles each register according to the year it starts. If the year is not completely covered by the register it will have a suffix of `a` or `b`. `a` is used if the register starts at the beginning of the year but does ends before it is over. `b` is used if the register does not start at the beginning of the year. Examples:

- "1907a" covers 9th Jan to 9th of Aug
- "1907b" covers 12th Aug to 31st Dec
- "1863b" covers 20th Feb 1863 to 29th Feb 1864. We do not have an "1863a"
- "1837" covers 3rd Jan to 30th Dec 1837

The data from each one of these collections is derived from separate pdf files for each register. These files contain photographic images of each page of that register. We extract textual data from these using `gemini-2.5-pro`, a multi-modal AI assistant. We access gemini via [Google AI Studio](https://aistudio.google.com/), and use custom prompts derived from those used by the [Archive Studio](https://github.com/mhumphries2323/Archive_Studio) project by Mark Humphries[^1], and which we will publish separately. 

This process generates a `csv` file which we can use with these scripts. If the `csv` file does not contain the right headers, the script will complain with a list of headers that it is expecting.

Usage:
```
uv run src/main.py register /folder/of/register.csv /output/folder/
```

[^1] Mark Humphries and Lianne C. Leddy, 2025. ArchiveStudio 1.0 Beta. Department of History: Wilfrid Laurier University.
