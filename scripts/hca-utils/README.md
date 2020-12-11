# Hca Utilities
A Python CLI to help make sure we don't have null file references or duplicate rows before creating snapshots.

## How to run
- Install [Poetry](https://python-poetry.org/) (make sure you're using python 3)
- Run `poetry install` to install dependencies
- Test it with `poetry run hca -V` to get the version

Before running it, you need to make sure your google application default credentials are all lined up.

TODO write a helper script/instructions to get that going, but the tl;dr is to
make sure you update the credentials paths in utils.py to point to valid application
default credential JSON files.

Example run:
`poetry run hca -e dev -p broad-jade-dev-data -d hca_dev_20201203`