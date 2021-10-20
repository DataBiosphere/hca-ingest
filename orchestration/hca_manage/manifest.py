"""
Parses a csv of staging areas intended for a DCP release and uploads to a
bucket for bulk ingest by our pipeline.
"""

import argparse
import csv
import logging
import sys
import warnings

import dagster
from dagster_graphql import DagsterGraphQLClient, ShutdownRepositoryLocationStatus, DagsterGraphQLClientError
from google.cloud.storage import Client, Bucket, Blob

from hca_manage.common import setup_cli_logging_format, query_yes_no


warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)


ETL_PARTITION_BUCKETS = {
    "dev": "broad-dsp-monster-hca-dev-etl-partitions",
    "prod": "broad-dsp-monster-hca-prod-etl-partitions"
}
REPOSITORY_LOCATION = "monster-hca-ingest"


def _sanitize_gs_path(path: str) -> str:
    return path.strip().strip("/")


def _parse_csv(csv_path: str) -> set[str]:
    paths = set()
    with open(csv_path, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                logging.debug("Empty path detected, skipping")
                continue

            assert len(row) == 1

            # sanitize and dedupe
            path = _sanitize_gs_path(row[0])
            assert path.startswith("gs://"), "Staging area path must start with gs:// scheme"
            paths.add(path)

    return paths


def parse_and_load_manifest(env: str, csv_path: str, release_tag: str, pipeline_name: str) -> None:
    paths = _parse_csv(csv_path)
    assert len(paths), "At least one import path is required"

    blob_name = f"{pipeline_name}/{release_tag}_manifest.csv"
    storage_client = Client()
    bucket: Bucket = storage_client.bucket(bucket_name=ETL_PARTITION_BUCKETS[env])
    blob = Blob(bucket=bucket, name=blob_name)
    if blob.exists():
        if not query_yes_no(f"Manifest {blob.name} already exists for pipeline {pipeline_name}, overwrite?"):
            return

    logging.info(f"Uploading manifest [bucket={bucket.name}, name={blob_name}]")
    blob.upload_from_string(data="\n".join(paths))


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


def enumerate_manifests(env: str) -> None:
    storage_client = Client()

    bucket: Bucket = storage_client.bucket(bucket_name=ETL_PARTITION_BUCKETS[env])
    blobs = bucket.list_blobs(prefix="load_hca")
    for blob in blobs:
        blob.reload()
        if not blob.size:
            continue

        logging.info(blob.name)


def load(args: argparse.Namespace) -> None:
    parse_and_load_manifest(args.env, args.csv_path, args.release_tag, "load_hca")
    parse_and_load_manifest(args.env, args.csv_path, args.release_tag, "validate_ingress")
    _reload_repository(_get_dagster_client())


def enumerate(args: argparse.Namespace) -> None:
    enumerate_manifests(args.env)


def reload(args: argparse.Namespace) -> None:
    logging.info(f"Reloading dagster user code env to reload partitions.")

    dagster_client: DagsterGraphQLClient = DagsterGraphQLClient("localhost", port_number=8080)
    _reload_repository(dagster_client)

    logging.info("Reload complete (it may take a few minutes before the repository is available again)")


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
    list_subparser.set_defaults(func=enumerate)

    reload_subparser = subparsers.add_parser("reload")
    reload_subparser.set_defaults(func=reload)

    args = parser.parse_args()
    args.func(args)
