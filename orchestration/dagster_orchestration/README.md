# Notes

This is an initial introduction of [Dagster](https://dagster.io)
into our codebase for workflow orchestration. This document captures notes,
todos and anything else we bump into as we test out this technology.

## Running
We're using poetry:
* from the `dagster` dir, run `poetry install`
* Run dagit and our toy pipeline via `poetry run dagit  -f hca_orchestration/pipelines.py`

The `stage_data` pipeline is being built to mimic our current HCA `stage_data` pipeline.

The `pre_process_metadata` solid kicks off a Beam job, either locally or in GCP depending
on which Dagster mode you're running in.

## Deployment notes
These are WIP steps + notes to getting code deployed.

* "User" code is deployed via a docker image. We build it using the Dockerfile located in this dir. It's currently
using `pip`, there is a TODO to move that to `poetry`.
* The `helmfile` located in `/ops/helmfiles/dagster/helmfile.yaml` controls the configuration of the Dagster
deployment. This is how we enumerate the "deployables"; right now we have a single deployable that is the
  monster-dagster image built by the above referenced image.
* The dagster deployment is configured to always pull a specific tag for this image.
  On merge to master and after CI passes, a new image will be pushed to GCR tagged with the current SHA of `master`.
  * Run `helmfile` against the new sha as well to update GKE: `ENV=dev GIT_SHORTHASH="<the hash that was pushed to GCR>" helmfile apply"
  * GKE should pick up the updated image and deploy
You must port forward to be able to access the `dagit` UI in our HCA GCP project: `kubectl port-forward --namespace dagster svc/monster-dagit 8080:80`
