import json
import argparse
from urllib.parse import urlparse

from dagster_utils.contrib.google import get_credentials
from dateutil import parser
from google.cloud import bigquery, storage

from hca_orchestration.solids.load_hca.non_file_metadata.load_non_file_metadata import NonFileMetadataTypes
from hca_orchestration.solids.load_hca.data_files.load_data_metadata_files import FileMetadataTypes


def inspect_entities_at_path(storage_client, bq_client, bq_project, bq_dataset, staging_area, prefix, entity_type):
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
            raise Exception("no links data found")

        print(f"No metadata for {entity_type} expected, skipping")
        return

    print(f"querying for metadata entities of type {entity_type}")
    entity_ids = metadata_entities.keys()
    query = f"""
    SELECT {entity_type}_id, content, version FROM `{bq_project}.{bq_dataset}.{entity_type}`
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

    print(f"*** All {entity_type} entities found ({len(metadata_entities.keys())} entities)")


def run(staging_area, bq_project, bq_dataset):
    creds = get_credentials()
    storage_client = storage.Client(project="broad-dsp-monster-hca-prod", credentials=creds)

    client = bigquery.Client(project=bq_project)
    inspect_entities_at_path(storage_client, client, bq_project, bq_dataset, staging_area, "", "links")
    for file_type in NonFileMetadataTypes:
        if file_type.value == 'links':
            continue
        inspect_entities_at_path(storage_client, client, bq_project, bq_dataset, staging_area, "metadata",
                                 file_type.value)

    for file_type in FileMetadataTypes:
        inspect_entities_at_path(storage_client, client, bq_project, bq_dataset, staging_area, "metadata",
                                 file_type.value)


if __name__ == '__main__':
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-s", "--staging-area", required=True)
    argparser.add_argument("-b", "--bq-project", required=True)
    argparser.add_argument("-d", "--bq-dataset", required=True)

    args = argparser.parse_args()

    run(args.staging_area, args.bq_project, args.bq_dataset)
