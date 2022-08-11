# HCA Ingest
Batch ETL workflow for ingesting HCA data into the Terra Data Repository (TDR). See the [architecture doc](https://github.com/DataBiosphere/hca-ingest/blob/master/ARCHITECTURE.md) for more
system design information.

# Getting Started

* Clone this repository to a local directory
* Build and run the dataflow tests
  * From the repository root: `sbt test` 
    * if this fails use `sbt test -Djava.security.manager=allow`
* Setup a local python environment
  * Make sure you have [poetry](https://python-poetry.org/docs/#installation) installed
  * From `orchestration/`:
    * Run `poetry install` to setup a local python virtual environment and install needed dependencies
      * It may be preferable to use `poetry lock --no-update` when updating the lockfile to avoid updating dependencies if you have already installed them.
      * FYI the first time you run this in your env, it can take up to 10 hours to complete, due to the large number of dependencies.
    * Run `pytest` and make sure all tests with the exception of our end-to-end suite run and pass locally

# Development Process
All code should first be developed on a branch off of the `master` branch. Once ready for review,
submit a PR against `master` and tag the `monster` team for review, and ensure all checks are passing.

Once approved and merged, the end-to-end test suite will be run. Once this passes, the dataflow
and orchestration code will be packaged into docker images for consumption by Dataflow and Dagster
respectively.

See the [deployment doc](https://github.com/DataBiosphere/hca-ingest/tree/master/ops/helmfiles) for next steps on getting code to dev and production.
