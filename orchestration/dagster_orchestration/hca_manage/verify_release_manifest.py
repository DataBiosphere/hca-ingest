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
import sys
from urllib.parse import urlparse

from google.cloud import bigquery, storage
from hca_orchestration.contrib import google as hca_google

logging.basicConfig(level=logging.INFO, format='%(message)s')


def get_expected_load_totals(storage_client, staging_areas):
    """
    Given a list of GS staging areas, count the files present in each /data subdir
    """
    expected = {}
    for staging_area in staging_areas:
        url = urlparse(staging_area)
        prefix = f"{url.path.lstrip('/')}/data/"
        blobs = list(storage_client.list_blobs(url.netloc, prefix=prefix))
        expected[staging_area] = len(blobs)
    return expected


def get_load_history(bq_project, dataset, start_date):
    client = bigquery.Client(project=bq_project)

    # Determine the number of distinct files loaded after the given start date, grouped by staging area
    query = f"""
                WITH base as (
                    SELECT distinct(source_name)
                    FROM `datarepo_{dataset}.datarepo_load_history` dlh
                    LEFT JOIN `datarepo_{dataset}.sequence_file` sf
                    ON sf.file_id = dlh.file_id
                    LEFT JOIN `broad-datarepo-terra-prod-hca2.datarepo_hca_prod_20201120_dcp2.analysis_file` af
                    ON af.file_id = dlh.file_id
                    LEFT JOIN `broad-datarepo-terra-prod-hca2.datarepo_hca_prod_20201120_dcp2.reference_file` rf
                    ON rf.file_id = dlh.file_id
                    LEFT JOIN `broad-datarepo-terra-prod-hca2.datarepo_hca_prod_20201120_dcp2.supplementary_file` supf
                    ON supf.file_id = dlh.file_id
                    LEFT JOIN `broad-datarepo-terra-prod-hca2.datarepo_hca_prod_20201120_dcp2.image_file` imgf
                    ON imgf.file_id = dlh.file_id
                    WHERE load_time >= '{start_date}'
                    AND state = 'succeeded'
                    AND (sf.file_id IS NOT NULL OR
                        af.file_id IS NOT NULL OR
                        rf.file_id IS NOT NULL OR
                        supf.file_id IS NOT NULL OR
                        imgf.file_id IS NOT NULL)
                )
                SELECT REGEXP_EXTRACT(source_name, '(gs://.*)/data/.*$') AS bucket, count(*) as cnt
                FROM base GROUP by bucket;
                """
    query_job = client.query(query)

    # hydrate the rows
    return [row for row in query_job]


def parse_manifest_file(manifest_file):
    with open(manifest_file) as manifest:
        # some of the staging areas submitted via the form need slight cleanup
        return [area.rstrip('\n/') for area in manifest]


def verify(start_date, manifest_file, gs_project, bq_project, dataset):
    logging.info("Parsing manifest and inspecting staging areas...")
    creds = hca_google.get_credentials()
    storage_client = storage.Client(project=gs_project, credentials=creds)
    staging_areas = parse_manifest_file(manifest_file)
    expected_load_totals = get_expected_load_totals(storage_client, staging_areas)

    logging.info("Inspecting load history in bigquery...")
    load_history = get_load_history(bq_project, dataset, start_date)
    tdr_load_totals = {
        load_history_row[0]: load_history_row[1]
        for load_history_row in load_history
    }

    for unexpected_area in tdr_load_totals.keys() - expected_load_totals.keys():
        logging.warning(f"⚠️ {unexpected_area} not in manifest but was imported")

    success = True
    total_loaded_count = 0
    total_expected_count = 0
    for area, expected_count in expected_load_totals.items():
        total_expected_count += expected_count
        if area in tdr_load_totals:
            total_loaded_count += tdr_load_totals[area]
            expected_count = expected_load_totals[area]
            if expected_count != tdr_load_totals[area]:
                logging.error(f"❌ Mismatched file count: staging_area = {area}, "
                              f"imported = {tdr_load_totals[area]}, expected = {expected_count}")
                success = False
            else:
                logging.info(
                    f"✅ staging_area = {area}, imported = {tdr_load_totals[area]}, expected = {expected_count}")
        else:
            logging.error(f"❌ {area} has not been imported")
            success = False
    logging.info('-' * 80)
    logging.info(f"Total files staged = {total_expected_count}, total files loaded = {total_loaded_count}")
    return success


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start-date", required=True)
    parser.add_argument("-f", "--manifest-file", required=True)
    parser.add_argument("-g", "--gs-project", required=True)
    parser.add_argument("-b", "--bq-project", required=True)
    parser.add_argument("-d", "--dataset", required=True)
    args = parser.parse_args()

    result = verify(args.start_date, args.manifest_file, args.gs_project, args.bq_project, args.dataset)
    if not result:
        sys.exit(1)
