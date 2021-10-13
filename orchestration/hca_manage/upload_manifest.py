import argparse
import csv
import logging
from google.cloud.storage import Client, Bucket, Blob

from hca_manage.common import setup_cli_logging_format, query_yes_no
from hca_orchestration.contrib.gcs import parse_gs_path

ETL_PARTITION_BUCKETS = {
    "dev": "broad-dsp-monster-hca-dev-etl-partitions",
    "prod": "broad-dsp-monster-hca-prod-etl-partitions"
}


def _sanitize_gs_path(path: str):
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


def parse_and_load_manifest(env: str, csv_path: str, release_tag: str):
    paths = _parse_csv(csv_path)
    assert len(paths), "At least one import path is required"

    blob_name = f"load_hca/{release_tag}_manifest.csv"
    storage_client = Client()
    bucket: Bucket = storage_client.bucket(bucket_name=ETL_PARTITION_BUCKETS[env])
    blob = Blob(bucket=bucket, name=blob_name)
    if blob.exists():
        if not query_yes_no(f"Manifest {blob.name} already exists, overwrite?"):
            return

    logging.info(f"Uploading manifest [bucket={bucket.name}, name={blob_name}]")
    blob.upload_from_string(data="\n".join(paths))


def load(args: argparse.Namespace) -> None:
    parse_and_load_manifest(args.env, args.csv_path, args.release_tag)


if __name__ == '__main__':
    setup_cli_logging_format()
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers()

    load_subparser = subparsers.add_parser("load")
    load_subparser.add_argument("-e", "--env", help="HCA environment", required=True)
    load_subparser.add_argument("-c", "--csv_path", help="CSV path", required=True)
    load_subparser.add_argument("-r", "--release_tag", help="DCP release tag", required=True)
    load_subparser.set_defaults(func=load)
    args = parser.parse_args()

    args.func(args)
