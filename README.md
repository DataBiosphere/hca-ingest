# HCA Ingest
Batch ETL workflow for ingesting HCA data into the Terra Data Repository (TDR). See the [architecture doc](https://github.com/DataBiosphere/hca-ingest/blob/master/ARCHITECTURE.md) for more
system design information.

# Getting Started

* Choose whether you will develop in a local virtual environment, like venv, or use the [Docker Compose dev env provided here](docker-compose.yaml).
* Clone this repository to a local directory
  * If you are running in a local virtual environment go ahead and set that up. Note that this project uses Python 3.9.12
    * Also install the [gcloud cloud command-line tool](https://cloud.google.com/sdk/docs/install) if you've not already done so.
  * If you are using the provided Docker Compose dev env use the follow command to invoke it: `docker compose run -w /hca-ingest app bash`
* Authenticate with gcloud using your Broad credentials `gcloud auth login`
* Then set up your billing project `gcloud config set project PROJECT_ID`
* You should also run `gcloud auth application-default login` to set up your default login for applications
* Build and run the dataflow tests
  * From the repository/image root: `sbt test` 
    * if this fails use `sbt test -Djava.security.manager=allow`
  * Make sure you have [poetry](https://python-poetry.org/docs/#installation) installed (already done in Docker image)
  * From `orchestration/`:
    * Run `poetry install` to setup a local python virtual environment and install needed dependencies
      * It may be preferable to use `poetry lock --no-update` when updating the lockfile to avoid updating dependencies if you have already installed them.
      * FYI the first time you run this in your env, it can take up to 10 hours to complete, due to the large number of dependencies.
    * Run `pytest` and make sure all tests with the exception of our end-to-end suite run and pass locally
      * If you installed pytest via poetry you will need to run `poetry run pytest` instead.

# Development Process
All code should first be developed on a branch off of the `master` branch. Once ready for review,
submit a PR against `master` and tag the `broad-data-ingest-admin` team for review, and ensure all checks are passing.

Once approved and merged, the end-to-end test suite will be run. Once this passes, the dataflow
and orchestration code will be packaged into docker images for consumption by Dataflow and Dagster
respectively.

See the [deployment doc](https://github.com/DataBiosphere/hca-ingest/tree/master/ops/helmfiles) for next steps on getting code to dev and production.
