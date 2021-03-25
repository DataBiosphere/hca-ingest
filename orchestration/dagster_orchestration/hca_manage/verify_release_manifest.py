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

import google.auth
from google.cloud import bigquery, storage
from urllib.parse import urlparse

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
                    FROM `datarepo_{dataset}.datarepo_load_history`
                    WHERE load_time >= '{start_date}'
                    AND state = 'succeeded'
                )
                SELECT REGEXP_EXTRACT(source_name, '(gs://.*)/data/.*$') AS bucket, count(*) as cnt
                FROM base GROUP by bucket;
                """
    query_job = client.query(query)

    # hydrate the rows
    return [row for row in query_job]


def parse_manifest_file(manifest_file):
    staging_areas = []
    with open(manifest_file) as manifest_file:
        for area in manifest_file:
            # some of the staging areas submitted via the form need slight cleanup
            staging_areas.append(area.rstrip('\n').rstrip('/'))
    return staging_areas


def verify(start_date, manifest_file, gs_project, bq_project, dataset):
    creds, _ = google.auth.default()

    logging.info("Parsing manifest and inspecting staging areas...")
    storage_client = storage.Client(project=gs_project, credentials=creds)
    staging_areas = parse_manifest_file(manifest_file)
    expected_load_totals = get_expected_load_totals(storage_client, staging_areas)

    logging.info("Inspecting load history in bigquery...")
    tdr_load_totals = {}
    load_history = get_load_history(bq_project, dataset, start_date)
    for row in load_history:
        tdr_load_totals[row[0]] = row[1]
        if row[0] not in expected_load_totals:
            logging.warning(f"⚠️ {row[0]} not in manifest but was imported")

    for area in expected_load_totals:
        if area in tdr_load_totals:
            expected_count = expected_load_totals[area]
            if expected_count != tdr_load_totals[area]:
                logging.error(f"❌ Mismatched file count: staging_area = {area}, "
                              f"imported = {tdr_load_totals[area]}, expected = {expected_count}")
            else:
                logging.info(
                    f"✅ staging_area = {area}, imported = {tdr_load_totals[area]}, expected = {expected_count}")
        else:
            logging.error(f"❌ {area} has not been imported")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start-date", required=True)
    parser.add_argument("-f", "--manifest-file", required=True)
    parser.add_argument("-g", "--gs-project", required=True)
    parser.add_argument("-b", "--bq-project", required=True)
    parser.add_argument("-d", "--dataset", required=True)
    args = parser.parse_args()

    verify(args.start_date, args.manifest_file, args.gs_project, args.bq_project, args.dataset)
