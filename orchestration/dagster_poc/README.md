# POC Notes

This is a proof-of-concept demonstrating how Dagster might be introduced
into our codebase for workflow orchestration. This document captures notes, 
todos and anything else we bump into as we test out this technology.

## Running
We're using poetry:
* from the `dagster_poc` dir, run `poetry install`
* Run dagit and our toy pipeline via `poetry run dagit  -f hca_proto/pipelines.py`

The `stage_data` pipeline is being build to mimic our current HCA `stage_data` pipeline.

The `pre_process_metadata` solid kicks off a k8s job. Make sure you have a k8s setup locally
(via Docker), and that you have an `hca` namespace setup.

## Deployment notes
These are WIP steps + notes to getting code deployed.

* "User" code is deployed via a docker image. We build it using the Dockerfile located in this dir. It's currently
using `pip`, there is a TODO to move that to `poetry`.
* The `helmfile` located in `/ops/helmfiles/dagster/helmfile.yaml` controls the configuration of the Dagster
deployment. This is how we enumerate the "deployables"; right now we have a single deployable that is the
  monster-dagster image built by the above referenced image.
* The dagster deployment is configured to always pull a specific tag for this image.
  To deploy updated dagster pipeline code:
  * Make sure you are configured to push to the `hca-dev` GCR.
  * Build a docker image, tagging with the current git SHA: `SHORTHASH="$(git rev-parse --short HEAD)" docker build . --build-arg DAGSTER_VERSION=0.10.4 -t us.gcr.io/broad-dsp-monster-hca-dev/monster-dagster:$SHORTHASH`
  * `SHORTHASH="$(git rev-parse --short HEAD)" docker push  us.gcr.io/broad-dsp-monster-hca-dev/monster-dagster:$SHORTHASH`
  * Run `helmfile` against the new sha as well to update GKE: `SHORTHASH="$(git rev-parse --short HEAD)" helmfile apply" 
  * GKE should pick up the updated image and deploy

You must port forward to be able to access the `dagit` UI: `kubectl port-forward --namespace dagster svc/monster-dagit 8080:80`
