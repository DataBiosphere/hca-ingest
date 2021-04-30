"""
Given a file containing a list of constituent staging dirs for a DCP release (aka a manifest),
verify that data has been loaded from each of them to the target DCP dataset and the count of loaded files
matches the # in the staging area

All datarepo load totals are inferred from a supplied start date for the import process.

Example invocation:
python verify_release_manifest.py -s 2021-03-24 -f testing.csv -g fake-gs-project -b fake-bq-project -d fake-dataset
"""
import argparse
import logging
from functools import partial

import sys
from urllib.parse import urlparse
from collections import defaultdict
from multiprocessing import Pool

from google.cloud import bigquery, storage
from google.cloud.bigquery.table import Row
from google.cloud.storage.client import Client
from hca_orchestration.contrib import google as hca_google
import json

logging.basicConfig(level=logging.INFO, format='%(message)s')

file_types = {
    'analysis_file',
    'sequence_file',
    'image_file',
    'reference_file',
    'sequence_file',
    'supplementary_file'
}


def get_expected_load_totals(storage_client: Client, staging_areas: set[str]) -> dict[str, set[str]]:
    """
    Given a list of GS staging areas, count the files present in each /data subdir
    """

    expected = defaultdict(set[str])
    for staging_area in staging_areas:
        url = urlparse(staging_area)
        for file_type in file_types:
            prefix = f"{url.path.lstrip('/')}/descriptors/{file_type}"
            blobs = list(storage_client.list_blobs(url.netloc, prefix=prefix))
            for blob in blobs:
                descriptor = json.loads(blob.download_as_text())
                target_path = f"/{descriptor['file_id']}/{descriptor['file_name']}"

                expected[staging_area].add(target_path)
    return expected


def find_files_in_load_history(bq_project: str, dataset: str, areas: dict[str, set]):
    client = bigquery.Client(project=bq_project)
    loaded_paths = defaultdict(set[str])
    for area, target_paths in areas.items():
        logging.debug(f"\tPulling loaded files for area {area}...")
        query = f"""
            SELECT distinct(target_path)
                        FROM `datarepo_{dataset}.datarepo_load_history` dlh
                        WHERE  state = 'succeeded'
                        and target_path IN UNNEST(@paths)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("paths", "STRING", target_paths),
            ]
        )
        query_job = client.query(query, job_config=job_config)
        loaded_paths[area] = {row[0] for row in query_job}
    return loaded_paths


def parse_manifest_file(manifest_file: str) -> list[str]:
    with open(manifest_file) as manifest:
        # some of the staging areas submitted via the form need slight cleanup
        return [area.rstrip('\n/') for area in manifest]


def process_staging_area(area: str, gs_project: str, bq_project: str, dataset: str, ):
    logging.debug(f"Processing staging area = {area}")
    creds = hca_google.get_credentials()
    storage_client = storage.Client(project=gs_project, credentials=creds)
    expected_load_totals = get_expected_load_totals(storage_client, {area})
    loaded_paths = find_files_in_load_history(bq_project, dataset, expected_load_totals)

    for a, target_paths in expected_load_totals.items():
        loaded_paths = loaded_paths[a]
        r = target_paths - loaded_paths
        if len(r) > 0:
            logging.warning(
                f"❌ area = {a} Mismatched loaded paths; expected to find {len(target_paths)}, found {len(loaded_paths)}")
            logging.warning(r)
        else:
            logging.info(f"✅  area = {a}, expected = {len(target_paths)}, found {len(loaded_paths)}, diff = {r}")


def verify(manifest_file: str, gs_project: str, bq_project: str, dataset: str, pool_size: int) -> bool:
    logging.info("Parsing manifest...")
    staging_areas = parse_manifest_file(manifest_file)
    logging.info(f"{len(staging_areas)} staging areas in manifest.")
    logging.info(f"Inspecting staging areas (pool_size = {pool_size})...")

    with Pool(pool_size) as p:
        curried = partial(process_staging_area, gs_project=gs_project, bq_project=bq_project, dataset=dataset)
        p.map(curried, staging_areas)

    return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--manifest-file", required=True)
    parser.add_argument("-g", "--gs-project", required=True)
    parser.add_argument("-b", "--bq-project", required=True)
    parser.add_argument("-d", "--dataset", required=True)
    parser.add_argument("-p", "--pool-size", type=int, default=4)
    args = parser.parse_args()

    result = verify(args.manifest_file, args.gs_project, args.bq_project, args.dataset, args.pool_size)
    if not result:
        sys.exit(1)
