"""
Utilities for working with a DCP release manifest (the set of staging areas intended for a DCP release)

This utility will:
* parse a csv of staging areas intended for a DCP release and upload to a
bucket for bulk ingest by our pipeline.
* enumerate any loaded manifests
* check on the ingest status of staging areas in a local manifest file
"""

import argparse
import csv
import json
import logging
import sys
import warnings

import dagster
import requests
from dagster_graphql import (
    DagsterGraphQLClient,
    DagsterGraphQLClientError,
    ShutdownRepositoryLocationStatus,
)
from google.cloud.storage import Blob, Bucket, Client

# isort: split

from hca_manage.common import query_yes_no, setup_cli_logging_format
from hca_orchestration.support.matchers import find_project_id_in_str

# isort: split

from more_itertools import chunked

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)


ETL_PARTITION_BUCKETS = {
    "dev": "broad-dsp-monster-hca-dev-etl-partitions",
    "prod": "broad-dsp-monster-hca-prod-etl-partitions"
}
# Test contains a single staging area for testing purposes
# The staging area is not used for any production pipelines
# Be sure to delete any snapshots and datasets created using this test staging area
STAGING_AREA_BUCKETS = {
    "prod": {
        "EBI": "gs://broad-dsp-monster-hca-prod-ebi-storage/prod",
        "UCSC": "gs://broad-dsp-monster-hca-prod-ebi-storage/prod",
        "LANTERN": "gs://broad-dsp-monster-hca-prod-lantern",
        "LATTICE": "gs://broad-dsp-monster-hca-prod-lattice/staging",
        "TEST": "gs://broad-dsp-monster-hca-prod-ebi-storage/broad_test_dataset"
    },
    "dev": {
        "EBI": "gs://broad-dsp-monster-hca-dev-ebi-staging/dev",
        "UCSC": "gs://broad-dsp-monster-hca-dev-ebi-staging/dev",
        "TEST": "gs://broad-dsp-monster-hca-prod-ebi-storage/broad_test_dataset"
    }
}
ENV_PIPELINE_ENDINGS = {
    "prod": "real_prod",
    "dev": "dev",
}
REPOSITORY_LOCATION = "monster-hca-ingest"
MAX_STAGING_AREAS_PER_PARTITION_SET = 20
RUN_STATUS_QUERY = """
            query FilteredRunsQuery {{
              pipelineRunsOrError(
                filter: {{
                  statuses: [SUCCESS]
                  tags: [
                    {{
                      key: "dagster/partition"
                      value: "{area}"
                    }}
                  ]
                }}
              ) {{
                __typename
                ... on PipelineRuns {{
                  results {{
                    runId
                  }}
                }}
              }}
            }}
            """


def _sanitize_gs_path(path: str) -> str:
    return path.strip().strip("/")


def _parse_csv(csv_path: str, env: str, project_id_only: bool = False,
               include_release_tag: bool = False, release_tag: str = "") -> tuple[list[list[str]], list[list[str]]]:
    keys = set()
    public_projects = set()
    with open(csv_path, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                logging.debug("Empty path detected, skipping")
                continue

            assert len(row) == 3, "CSV must have 3 columns: institution, project_id, and Yes or No for Public"
            row = [x.strip() for x in row]
            institution = row[0].upper()
            project_id = find_project_id_in_str(row[1])
            public = row[2].lower()

            if project_id_only:
                project_id = row[1]
                key = project_id
            else:
                if institution not in STAGING_AREA_BUCKETS[env]:
                    raise Exception(f"Unknown institution {institution} found")

                institution_bucket = STAGING_AREA_BUCKETS[env][institution]
                path = institution_bucket + "/" + project_id

                # sanitize and dedupe
                path = _sanitize_gs_path(path)
                assert path.startswith("gs://"), "Staging area path must start with gs:// scheme"
                key = path

            if include_release_tag:
                key = key + f",{release_tag}"
            keys.add(key)

            # make a separate set of public projects
            if public == "no":
                public_projects.add(key)

    chunked_paths = chunked(keys, MAX_STAGING_AREAS_PER_PARTITION_SET)
    chunked_paths_no_ma = chunked(public_projects, MAX_STAGING_AREAS_PER_PARTITION_SET)
    return [chunk for chunk in chunked_paths], [chunk for chunk in chunked_paths_no_ma]


def parse_and_load_manifest(env: str, csv_path: str, release_tag: str,
                            pipeline_name: str, project_id_only: bool = False,
                            include_release_tag: bool = False, no_ma: bool = False) -> None:
    chunked_paths = _parse_csv(csv_path, env, project_id_only, include_release_tag, release_tag)
    paths_to_use = chunked_paths[1] if no_ma else chunked_paths[0]
    storage_client = Client()
    bucket: Bucket = storage_client.bucket(bucket_name=ETL_PARTITION_BUCKETS[env])

    for pos, chunk in enumerate(paths_to_use):
        assert len(chunk), "At least one import path is required"
        qualifier = chr(pos + 97)  # dcp11_a, dcp11_b, etc.
        blob_name = f"{pipeline_name}/{release_tag}_{qualifier}_manifest.csv"
        blob = Blob(bucket=bucket, name=blob_name)
        if blob.exists():
            if not query_yes_no(f"Manifest {blob.name} already exists for pipeline {pipeline_name}, overwrite?"):
                return

        logging.info(f"Uploading manifest [bucket={bucket.name}, name={blob_name}]")
        blob.upload_from_string(data="\n".join(chunk))


def _get_dagster_client() -> DagsterGraphQLClient:
    try:
        return DagsterGraphQLClient("localhost", port_number=8080)
    except DagsterGraphQLClientError:
        logging.error("Could not connect to dagster instance on port 8080, ensure you are port forwarding")
        sys.exit(1)


def _reload_repository(dagster_client: DagsterGraphQLClient) -> None:
    result = dagster_client.shutdown_repository_location(REPOSITORY_LOCATION)
    if result.status != ShutdownRepositoryLocationStatus.SUCCESS:
        logging.error(f"Error reloading user code repository: {result.message}")
        sys.exit(1)


def _enumerate_manifests(env: str) -> None:
    storage_client = Client()

    bucket: Bucket = storage_client.bucket(bucket_name=ETL_PARTITION_BUCKETS[env])
    blobs = bucket.list_blobs(prefix="load_hca")
    for blob in blobs:
        blob.reload()
        if not blob.size:
            continue

        logging.info(blob.name)


def load(args: argparse.Namespace) -> None:
    parse_and_load_manifest(args.env, args.csv_path, args.release_tag, "per_project_load_hca")
    parse_and_load_manifest(args.env, args.csv_path, args.release_tag, "validate_ingress")
    parse_and_load_manifest(
        args.env,
        args.csv_path,
        args.release_tag,
        f"cut_project_snapshot_job_{ENV_PIPELINE_ENDINGS[args.env]}",
        project_id_only=True,
        include_release_tag=True
    )
    # also load the manifest for the make_snapshot_public pipeline - FE-39 Interim Managed Access Solution
    parse_and_load_manifest(
        args.env,
        args.csv_path,
        args.release_tag,
        f"make_snapshot_public_job_{ENV_PIPELINE_ENDINGS[args.env]}",
        project_id_only=True,
        include_release_tag=True,
        no_ma=True
    )
    _reload_repository(_get_dagster_client())


def enumerate_manifests(args: argparse.Namespace) -> None:
    _enumerate_manifests(args.env)


def reload(args: argparse.Namespace) -> None:
    logging.info("Reloading dagster user code env to reload partitions.")

    dagster_client: DagsterGraphQLClient = DagsterGraphQLClient("localhost", port_number=8080)
    _reload_repository(dagster_client)

    logging.info("Reload complete (it may take a few minutes before the repository is available again)")


def status(args: argparse.Namespace) -> None:
    paths = []
    with open(args.csv_path, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            paths.append(row[0])

    for area in paths:
        body = {
            "operationName": "FilteredRunsQuery", "variables": {}, "query": RUN_STATUS_QUERY.format(area=area)
        }
        response = requests.post(
            "http://localhost:8080/graphql",
            data=json.dumps(body),
            headers={
                "content-type": "application/json"})
        runs = response.json()['data']['pipelineRunsOrError']['results']
        if not runs:
            logging.error(f"{area}\t<no successful runs>")
        else:
            logging.error(f"{area}\t{runs[0]['runId']}")


if __name__ == '__main__':
    setup_cli_logging_format()
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers()

    load_subparser = subparsers.add_parser("load")
    load_subparser.add_argument("-e", "--env", help="HCA environment", required=True)
    load_subparser.add_argument("-c", "--csv_path", help="CSV path", required=True)
    load_subparser.add_argument("-r", "--release_tag", help="DCP release tag", required=True)
    load_subparser.set_defaults(func=load)

    list_subparser = subparsers.add_parser("enumerate")
    list_subparser.add_argument("-e", "--env", help="HCA environment", required=True)
    list_subparser.set_defaults(func=enumerate_manifests)

    reload_subparser = subparsers.add_parser("reload")
    reload_subparser.set_defaults(func=reload)

    status_subparser = subparsers.add_parser("status")
    status_subparser.add_argument("-c", "--csv_path", help="CSV path", required=True)
    status_subparser.set_defaults(func=status)

    args = parser.parse_args()
    args.func(args)
