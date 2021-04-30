"""
Given a file containing a list of constituent staging dirs for a DCP release (aka a manifest),
verify that data has been loaded from each of them to the target DCP dataset and the count of loaded files
matches the # in the staging area.

Files are determined to be loaded if they exist at the desired target path and crc as defined in the staging
areas descriptors.

Example invocation:
python verify_release_manifest.py -f testing.csv -g fake-gs-project -b fake-bq-project -d fake-dataset
"""
import argparse
import json
import logging
import sys
from collections import defaultdict, namedtuple
from functools import partial
from multiprocessing import Pool
from urllib.parse import urlparse

from google.cloud import bigquery, storage
from google.cloud.storage.client import Client
from hca_orchestration.contrib import google as hca_google

logging.basicConfig(level=logging.INFO, format='%(message)s')

PathWithCrc = namedtuple('PathWithCrc', 'path crc')

FILE_TYPES = {
    'analysis_file',
    'sequence_file',
    'image_file',
    'reference_file',
    'sequence_file',
    'supplementary_file'
}


def get_staging_area_file_descriptors(storage_client: Client, staging_areas: set[str]) -> dict[str, set[str]]:
    """
    Given a set of GS staging areas, return the descriptors present in each area
    """

    expected = defaultdict(set[str])
    for staging_area in staging_areas:
        url = urlparse(staging_area)
        for file_type in FILE_TYPES:
            prefix = f"{url.path.lstrip('/')}/descriptors/{file_type}"
            blobs = list(storage_client.list_blobs(url.netloc, prefix=prefix))
            for blob in blobs:
                expected[staging_area].add(blob.download_as_text())

    return expected


def target_path_from_descriptor(descriptor: dict):
    return f"/{descriptor['file_id']}/{descriptor['file_name']}"


def find_files_in_load_history(bq_project: str, dataset: str, areas: dict[str, set[PathWithCrc]]):
    client = bigquery.Client(project=bq_project)
    loaded_paths = {}

    for area, raw_descriptors in areas.items():
        logging.debug(f"\tPulling loaded files for area {area}...")
        target_paths = [target_path_from_descriptor(json.loads(raw_descriptor)) for raw_descriptor in raw_descriptors]
        query = f"""
            SELECT target_path, checksum_crc32c
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
        loaded_paths[area] = {PathWithCrc(row["target_path"], row["checksum_crc32c"]) for row in query_job}

    return loaded_paths


def parse_manifest_file(manifest_file: str) -> list[str]:
    with open(manifest_file) as manifest:
        # some of the staging areas submitted via the form need slight cleanup
        return [area.rstrip('\n/') for area in manifest]


def process_staging_area(area: str, gs_project: str, bq_project: str, dataset: str, ):
    logging.debug(f"Processing staging area = {area}")
    creds = hca_google.get_credentials()
    storage_client = storage.Client(project=gs_project, credentials=creds)
    expected_load_totals = get_staging_area_file_descriptors(storage_client, {area})
    loaded_paths_by_staging_area = find_files_in_load_history(bq_project, dataset, expected_load_totals)

    for a, raw_descriptors in expected_load_totals.items():
        target_paths_with_crcs = set()
        for raw_descriptor in raw_descriptors:
            parsed = json.loads(raw_descriptor)
            target_paths_with_crcs.add(PathWithCrc(target_path_from_descriptor(parsed), parsed["crc32c"]))

        load_paths_for_staging_area = {PathWithCrc(path[0], path[1]) for path in loaded_paths_by_staging_area[a]}
        r = target_paths_with_crcs - load_paths_for_staging_area
        if len(r) > 0:
            logging.warning(
                f"❌ area = {a} Mismatched loaded paths; expected to find {len(target_paths_with_crcs)}, found {len(load_paths_for_staging_area)}")
            logging.warning(r)
        else:
            logging.info(
                f"✅  area = {a}, expected = {len(target_paths_with_crcs)}, found {len(load_paths_for_staging_area)}, diff = {r}")


def verify(manifest_file: str, gs_project: str, bq_project: str, dataset: str, pool_size: int) -> bool:
    logging.info("Parsing manifest...")
    staging_areas = parse_manifest_file(manifest_file)
    logging.info(f"{len(staging_areas)} staging areas in manifest.")
    logging.info(f"Inspecting staging areas (pool_size = {pool_size})...")

    frozen = partial(process_staging_area, gs_project=gs_project, bq_project=bq_project, dataset=dataset)
    if pool_size > 0:
        with Pool(pool_size) as p:
            p.map(frozen, staging_areas)
    else:
        for area in staging_areas:
            frozen(area)

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
