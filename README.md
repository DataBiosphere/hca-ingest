# HCA Ingest
Batch ETL workflow for ingesting HCA data into the Terra Data Repository (TDR). See the [architecture doc](https://github.com/DataBiosphere/hca-ingest/blob/main/ARCHITECTURE.md) for more
system design information.

# Getting Started

* Choose whether you will develop in a local virtual environment, like venv, or use the [Docker Compose dev env provided here](docker-compose.yaml).
* Clone this repository to a local directory and create a new dev branch, ideally named after your Jira ticket.
  * If you are running in a local virtual environment go ahead and set that up. Note that this project uses Python 3.9.12
    * Also install the [gcloud cloud command-line tool](https://cloud.google.com/sdk/docs/install) if you've not already done so.
  * If you are using the provided Docker Compose dev env use the follow command to invoke it: `docker compose run -w /hca-ingest app bash`
    * _Note that if you are not already logged in to gcloud, you will need to do so before running \
      the docker compose command, as this will pull the latest image from Artifact Registry._
* Authenticate with gcloud using your Broad credentials `gcloud auth login`
* Then set up your billing project `gcloud config set project PROJECT_ID`
* Then set up your default login for applications `gcloud auth application-default login`
* Build and run the dataflow tests
  * From the repository/image root: `sbt test` 
    * if this fails you may need to get a clean clone of the repository
    * Note that this will take a while to run, as it will build the project and run all tests
  * Make sure you have [poetry](https://python-poetry.org/docs/#installation) installed **(already done in Docker image)**
  * From `orchestration/`:
    * Run `poetry install` to set up a local python virtual environment and install needed dependencies
      * If you've updated the pyproject.toml you'll need to update the lock file first. \
      It may be preferable to use `poetry lock --no-update` when updating the lockfile to avoid updating dependencies 
      unintentionally.
      * FYI the first time you run this in your env, it can take up to 10 hours to complete, 
      due to the large number of dependencies.
        * _This is not true for the Docker image, which will make use of the poetry cache._
    * Run `pytest` and make sure all tests except our end-to-end suite run and pass locally
      * If you installed pytest via poetry you will need to run `poetry run pytest` instead.

# Development Process
All code should first be developed on a branch off of the `main` branch. Once ready for review, \
submit a PR against `main` and tag the `broad-data-ingest-admin` team for review, and ensure all checks are passing.

If you've updated the Docker image at the top of the repository here, you will need to build & push it to Artifact Registry, \
using `update_docker_image.sh`. First update the version field, then run the script. \
This will build the image, tag it with the version, and push the image to Artifact Registry. \
Note that this may take a bit to establish the connection to Artifact Registry, so be patient. \
_It may be that you are on the split VPN and/or trying to push over IPv6. Either turn off the VPN or turn of IPV6 \
on your router to speed this up._

Once approved and merged, the end-to-end test suite will be run. Once this passes, the dataflow \
and orchestration code will be packaged into docker images for consumption by Dataflow and Dagster
respectively.

See the [deployment doc](https://github.com/DataBiosphere/hca-ingest/tree/main/ops/helmfiles) for next steps on getting code to dev and production.
