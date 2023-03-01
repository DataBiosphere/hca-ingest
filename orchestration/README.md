# Overview

This module contains orchestration code necessary for importing data to TDR from HCA staging areas. The orchestration framework we use is [Dagster](https://dagster.io).

## Setting up your dev environment
* see the [root readme](../README.md) if this is your first time setting up a dev environment
* Make sure you have [Poetry](https://python-poetry.org/docs/#installation) installed.
* From the `orchestration` dir, run `poetry install` to install all dependencies.
* Run `poetry run pre-commit install` to install a hook that will automatically lint your code whenever you create a commit.
	* If the hook blocks you from creating a commit, you can just run `git commit` again, unless it reports any linting errors it wasn't able to fix.

## Running
Once you have your environment set up verify that you can load our repository of pipelines in the web console:
* See the [ops README](../ops/helmfiles/README.md) for instructions on how to set up your environment to run Dagster locally.

<!-- This doesn't work
* Enter the virtual environment that was setup above: `poetry shell`
* Run dagit and point it at the local dev repository:
  * `dagit -f hca_orchestration/repositories/dev_repositories.py`
* View the web console at http://localhost:3000 and run of the `load_hca` pipeline in test mode -->

## Testing
* Run unit tests via at the root of the `orchestration` module `pytest -v`.
* End-to-end tests run after merge to `master`. These are also runnable locally:
  * `pytest -m e2e hca_orchestration/tests/e2e`
  * You must have a Google account with sufficient permissions to the `dev` TDR environment to run the e2e tests.

## Pipelines
* *load_hca* : Primary pipeline for importing data to HCA
* *copy_project*: Copies a single HCA project from one dataset to another
* *validate_ingress*: Validates an HCA staging area
* *cut_snapshot*: Submits an HCA snapshot creation job to TDR and polls to completion

All pipelines are runnable in local development configurations.

## Tools
The [hca_manage](https://github.com/DataBiosphere/hca-ingest/tree/master/orchestration/hca_manage) directory contains useful command-line tools for working with and validating HCA datasets, staging areas and snapshots. 

## Deployment notes
See the [deployment documentation](../ops/helmfiles/README.md).
