# Hca Manage
A Python CLI to help make sure we don't have null file references or duplicate rows before creating snapshots.

## How to run
- Install [Poetry](https://python-poetry.org/) (make sure you're using python 3)
- Run `poetry install` from the dagster_orchestration directory to install dependencies
- Test it with `poetry run check -V` to get the version

Before running it, you need to make sure your google application default credentials are all lined up.

Do so by running `gcloud auth application-default login` and logging in with the appropriate account.

Example run:
`poetry run check -e dev -d hca_dev_20201203`

To actually remove things, add the remove `-r` flag after:
`poetry run check -e dev -d hca_dev_20201203 -r`

Be very sure and careful when adding the remove flag.

Example of snapshot creation:
`poetry run snapshot -e dev -c -d hca_dev_20201203 -q mytest1`

Example of dataset removal:
`poetry run dataset -e dev -r -n myfakedataset`

Example of soft deletion:
`poetry run soft_delete -e dev -p path/to/csv -t myfaketable -j myfakeproject -d myfakedataset`
