"""
Given a file containing a list of constituent staging dirs for a DCP release (aka a manifest),
verify that data has been loaded from each of them to the target DCP dataset and the count of loaded files
matches the # in the staging area.

Files are determined to be loaded if they exist at the desired target path and crc as defined in the staging
areas descriptors. It's possible that an expected file was loaded by another staging dir (i.e,. they both
contain the same file). While this is discouraged, it's technically possible and we need to accommodate that.
So, we check if the target path was loaded, disregarding the source staging dir.

Additonally, this will check that metadata was loaded properly (including links) by pull the entity_id, version and
content from the files in GS and checking that the expected row is present in the given dataset.

Example invocation:
python verify_release_manifest.py -f testing.csv -g fake-gs-project -b fake-bq-project -d fake-dataset
"""
import argparse
import json
import logging
import sys
from dataclasses import dataclass
from collections import defaultdict
from functools import partial
from multiprocessing import Pool
from urllib.parse import urlparse
from dateutil import parser

from google.cloud import bigquery, storage
from google.cloud.storage.client import Client
from dagster_utils.contrib.google import get_credentials

from hca_orchestration.solids.load_hca.data_files.load_data_metadata_files import FileMetadataTypes
from hca_orchestration.solids.load_hca.non_file_metadata.load_non_file_metadata import NonFileMetadataTypes

logging.basicConfig(level=logging.INFO, format='%(message)s')


@dataclass(frozen=True)
class PathWithCrc:
    path: str
    crc32c: str


def get_staging_area_file_descriptors(storage_client: Client, staging_areas: set[str]) -> dict[str, set[PathWithCrc]]:
    """
    Given a set of GS staging areas, return the downloaded descriptors present in each area
    """

    expected: dict[str, set[PathWithCrc]] = defaultdict(set[PathWithCrc])
    for staging_area in staging_areas:
        url = urlparse(staging_area)

        for file_type in FileMetadataTypes:
            prefix = f"{url.path.lstrip('/')}/descriptors/{file_type.value}"
            blobs = list(storage_client.list_blobs(url.netloc, prefix=prefix))
            for blob in blobs:
                parsed = json.loads(blob.download_as_text())
                path_with_crc = PathWithCrc(target_path_from_descriptor(parsed), parsed["crc32c"])
                expected[staging_area].add(path_with_crc)

    return expected


def target_path_from_descriptor(descriptor: dict[str, str]) -> str:
    return f"/{descriptor['file_id']}/{descriptor['file_name']}"


def find_files_in_load_history(bq_project: str, dataset: str,
                               areas: dict[str, set[PathWithCrc]]) -> dict[str, set[PathWithCrc]]:
    client = bigquery.Client(project=bq_project)
    loaded_paths = {}

    for area, paths_with_crc in areas.items():
        logging.debug(f"\tPulling loaded files for area {area}...")
        target_paths = [path_with_crc.path for path_with_crc in paths_with_crc]
        query = f"""
            SELECT target_path, checksum_crc32c
            FROM `datarepo_{dataset}.datarepo_load_history` dlh
            WHERE  state = 'succeeded'
            AND target_path IN UNNEST(@paths)
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


def process_staging_area(area: str, gs_project: str, bq_project: str, dataset: str) -> None:
    logging.debug(f"Processing staging area = {area}")

    creds = get_credentials()
    storage_client = storage.Client(project=gs_project, credentials=creds)
    expected_loaded_paths = get_staging_area_file_descriptors(storage_client, {area})
    loaded_paths_by_staging_area = find_files_in_load_history(bq_project, dataset, expected_loaded_paths)

    for area, paths_with_crc in expected_loaded_paths.items():
        load_paths_for_staging_area = loaded_paths_by_staging_area[area]
        diff = paths_with_crc - load_paths_for_staging_area
        loaded = len(load_paths_for_staging_area)
        staged = len(paths_with_crc)

        if diff:
            logging.warning(
                f"❌ area = {area} - (data files) Mismatched loaded paths; expected files loaded = {staged}, actual loaded = {loaded}"
            )
            logging.warning(diff)
        else:
            logging.info(
                f"✅ area = {area} - (data files) expected files loaded = {staged}, actual loaded = {loaded}")

        verify_metadata(area, bq_project, dataset)


def inspect_entities_at_path(storage_client: Client, bq_client: bigquery.Client, bq_project: str,
                             bq_dataset: str, staging_area: str, prefix: str, entity_type: str) -> None:
    metadata_entities = {}

    url = urlparse(staging_area)
    if prefix:
        prefix = f"{url.path.lstrip('/')}/{prefix}/{entity_type}"
    else:
        prefix = f"{url.path.lstrip('/')}/{entity_type}"

    blobs = list(storage_client.list_blobs(url.netloc, prefix=prefix))

    for blob in blobs:
        content = blob.download_as_text()
        file_name = blob.name.split('/')[-1]
        entity_id = file_name.split('_')[0]
        version = file_name.split('_')[1].replace('.json', '')
        metadata_entities[entity_id] = (version, content)

    if len(metadata_entities) == 0:
        if entity_type == 'links':
            raise Exception(f"❌ area = {staging_area} no links data found")

        logging.debug(f"❌ area = {staging_area} No metadata for {entity_type} expected, skipping")
        return

    logging.debug(f"querying for metadata entities of type {entity_type}")
    entity_ids = metadata_entities.keys()
    query = f"""
    SELECT {entity_type}_id, content, version FROM `{bq_project}.datarepo_{bq_dataset}.{entity_type}`
    WHERE {entity_type}_id IN UNNEST(@entity_ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("entity_ids", "STRING", entity_ids),
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)
    rows = {row[f'{entity_type}_id']: (row['version'], row['content']) for row in query_job.result()}

    for key, (version, content) in metadata_entities.items():
        assert key in rows.keys(), f"{entity_type} ID {key} not in table"
        row = rows[key]
        assert parser.parse(version) == row[0], f"{entity_type} ID {key} version is incorrect"
        assert json.loads(content) == json.loads(row[1]), f"{entity_type} ID {key} content is incorrect"

    logging.info(
        f"✅ area = {staging_area} - (metadata) all {entity_type} entities found ({len(metadata_entities.keys())} entities)")


def verify_metadata(staging_area: str, bq_project: str, bq_dataset: str) -> None:
    creds = get_credentials()
    storage_client = storage.Client(project="broad-dsp-monster-hca-prod", credentials=creds)

    client = bigquery.Client(project=bq_project)
    inspect_entities_at_path(storage_client, client, bq_project, bq_dataset, staging_area, "", "links")
    for non_file_metadata_type in NonFileMetadataTypes:
        if non_file_metadata_type.value == 'links':
            continue
        inspect_entities_at_path(storage_client, client, bq_project, bq_dataset, staging_area, "metadata",
                                 non_file_metadata_type.value)

    for file_metadata_type in FileMetadataTypes:
        inspect_entities_at_path(storage_client, client, bq_project, bq_dataset, staging_area, "metadata",
                                 file_metadata_type.value)


def verify(manifest_file: str, gs_project: str, bq_project: str, dataset: str, pool_size: int) -> bool:
    logging.info("Parsing manifest...")
    staging_areas = parse_manifest_file(manifest_file)
    logging.info(f"{len(staging_areas)} staging areas in manifest.")
    logging.info(f"Inspecting staging areas (pool_size = {pool_size})...")

    # we multiprocess because this takes quite awhile for > 10 projects, which is common for our releases
    frozen = partial(process_staging_area, gs_project=gs_project, bq_project=bq_project, dataset=dataset)
    if pool_size > 0:
        with Pool(pool_size) as p:
            p.map(frozen, staging_areas)
    else:
        for area in staging_areas:
            frozen(area)

    return True


if __name__ == '__main__':
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-f", "--manifest-file", required=True)
    argparser.add_argument("-g", "--gs-project", required=True)
    argparser.add_argument("-b", "--bq-project", required=True)
    argparser.add_argument("-d", "--dataset", required=True)
    argparser.add_argument("-p", "--pool-size", type=int, default=4)
    args = argparser.parse_args()

    result = verify(args.manifest_file, args.gs_project, args.bq_project, args.dataset, args.pool_size)
    if not result:
        sys.exit(1)
