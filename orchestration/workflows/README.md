# Argo Workflows
This directory contains Argo workflows for HCA ingestion tasks.

## Setup

* Ensure you have `kubectl` setup and pointing at the appropriate k8s cluster
* Follow the steps on this page to download and install a stable release of the `argo` binary from [this page](https://github.com/argoproj/argo/releases).

## Submitting a Job
Jobs can be submitted via the `argo` CLI like so from the root of your checkout:

`argo submit <path to workflow yaml> -p param1="value1" -p param2="value2"`

For instance:

`argo submit orchestration/workflows/dev/run-import-hca-total.yaml -p source-bucket-name="foo" 
-p source-bucket-prefix="bar" -p staging-bucket-prefix="baz" -p dataset-id="some_uuid" -p data-repo-name="some_repo_name`
