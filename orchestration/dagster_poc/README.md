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

## TODOs
- [ ] Figure out how to long poll k8s jobs in Dagster
- [ ] Kickoff and run a dataflow job to completion.
- [ ] Exercise failure modes (job kickoff failure? dataflow failure? etc.)
- [ ] Where should config live? (local, hca_dev, prod, etc.)