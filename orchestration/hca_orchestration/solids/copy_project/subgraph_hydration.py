import json
import logging
from collections import defaultdict
from urllib.parse import urlparse

import requests
from dagster import In, Nothing, op
from dagster.core.execution.context.compute import (
    AbstractComputeExecutionContext,
)
from dagster_utils.contrib.google import get_credentials
from google.auth.transport.requests import Request
from google.cloud.bigquery import ArrayQueryParameter, Row
from google.oauth2.credentials import Credentials
from more_itertools import chunked
from requests.structures import CaseInsensitiveDict

from hca_orchestration.contrib.bigquery import BigQueryService
from hca_orchestration.models.entities import (
    DataFileEntity,
    MetadataEntity,
    build_subgraph_from_links_row,
)
from hca_orchestration.models.hca_dataset import TdrDataset
from hca_orchestration.models.scratch import ScratchConfig
from hca_orchestration.resources.hca_project_config import (
    HcaProjectCopyingConfig,
)
from hca_orchestration.support.subgraphs import build_subgraph_nodes
from hca_orchestration.support.typing import MetadataType

BQ_CHUNK_SIZE = 20000


@op(
    required_resource_keys={
        "bigquery_service",
        "hca_project_copying_config",
        "scratch_config",
        "target_hca_dataset"
    },
    ins={"start": In(Nothing)}
)
def hydrate_subgraphs(context: AbstractComputeExecutionContext) -> set[DataFileEntity]:
    # 1. given a project ID, query the links table for all rows associated with the project
    # 2. find all process entries assoc. with the links
    # 3. find all other entities assoc. with the links
    bigquery_service: BigQueryService = context.resources.bigquery_service
    hca_project_config: HcaProjectCopyingConfig = context.resources.hca_project_copying_config
    project_id = hca_project_config.source_hca_project_id
    scratch_config: ScratchConfig = context.resources.scratch_config
    target_hca_dataset: TdrDataset = context.resources.target_hca_dataset
    scratch_bucket_name = f"{scratch_config.scratch_bucket_name}/{scratch_config.scratch_prefix_name}"

    # fetch the links rows that make up this project and then build the subgraphs in memory
    # we will then traverse the subgraphs and pull out all related entities and any files they refer to
    query = f"""
      SELECT *
        FROM `{hca_project_config.source_bigquery_project_id}.{hca_project_config.source_snapshot_name}.links`
        WHERE project_id = "{project_id}"
    """
    rows = bigquery_service.run_query(
        query,
        hca_project_config.source_bigquery_project_id,
        hca_project_config.source_bigquery_region
    )
    links = [
        build_subgraph_from_links_row(row) for row in rows
    ]
    nodes = build_subgraph_nodes(links)

    context.log.info("Saving entities to extract scratch path...")
    if 'project' not in nodes:
        # some subgraphs don't explicitly include the parent project
        nodes[MetadataType('project')].append(
            MetadataEntity(MetadataType('project'), hca_project_config.source_hca_project_id)
        )

    # save metadata entities to our staging area
    _extract_entities_to_path(
        nodes,
        f"{scratch_bucket_name}/tabular_data_for_ingest",
        hca_project_config.source_bigquery_project_id,
        hca_project_config.source_snapshot_name,
        hca_project_config.source_bigquery_region,
        bigquery_service
    )

    # fetch file metadata entities so we can determine the access URL for extraction to our
    # staging area downstream
    context.log.info("Determining files to load...")
    entity_rows: dict[str, list[Row]] = _fetch_file_entities(
        nodes,
        hca_project_config.source_bigquery_project_id,
        hca_project_config.source_snapshot_name,
        hca_project_config.source_bigquery_region,
        bigquery_service
    )

    # construct a target path from each file entity row
    target_path_to_drs_uri = {}
    for entity_type, rows in entity_rows.items():
        if not entity_type.endswith("_file"):
            continue

        for row in rows:
            # file_id is actually a DRS URI in snapshots (vs a uuid in the origin dataset)
            drs_uri = row['file_id']
            descriptor = json.loads(row['descriptor'])
            data_file_id = descriptor['file_id']
            file_name = descriptor['file_name']
            crc32c = descriptor['crc32c']

            target_path = f"/v1/{data_file_id}/{crc32c}/{file_name}"
            target_path_to_drs_uri[target_path] = drs_uri

    # determine which target paths have already been loaded to save ourselves some work
    previously_loaded = _find_previously_loaded_target_paths(
        set(target_path_to_drs_uri.keys()),
        target_hca_dataset,
        bigquery_service
    )

    data_entities = set()
    creds = _get_credentials()
    context.log.info(f"Resolving DRS object GCS paths ({len(target_path_to_drs_uri)} objects)...")

    with requests.Session() as s:
        for cnt, (target_path, drs_uri) in enumerate(target_path_to_drs_uri.items(), start=1):
            if target_path in previously_loaded:
                context.log.debug(f"Skipping load of {target_path}, already loaded")
                continue

            if cnt % 100 == 0:
                context.log.info(f"Resolved {cnt} paths...")

            s.headers = CaseInsensitiveDict[str]()
            s.headers["Authorization"] = f"Bearer {creds.token}"

            if creds.expired:
                # our refresh token expires after 1/2 hour
                context.log.info("Refreshing expired credentials")
                creds = _get_credentials()
                s.headers["Authorization"] = f"Bearer {creds.token}"

            drs_object_id = urlparse(drs_uri).path[1:]
            drs_host = urlparse(drs_uri).netloc

            access_url = _fetch_drs_access_info(drs_host, drs_object_id, s)
            data_entities.add(DataFileEntity(access_url, target_path))

    return data_entities


def _find_previously_loaded_target_paths(
        target_paths: set[str],
        target_hca_dataset: TdrDataset,
        bigquery_service: BigQueryService,
        chunk_size: int = BQ_CHUNK_SIZE
) -> set[str]:
    chunked_target_paths = chunked(target_paths, chunk_size)
    logging.info(f"Searching for previously loaded paths, split paths set of size {len(target_paths)}")
    previously_loaded = set()
    for cnt, chunk in enumerate(chunked_target_paths):
        logging.info(f"Chunk {cnt}")
        query = f"""
        SELECT target_path FROM  `{target_hca_dataset.project_id}.datarepo_{target_hca_dataset.dataset_name}.datarepo_load_history`
        WHERE target_path IN UNNEST(@target_paths_chunk)
        AND state = 'succeeded'
        """
        query_params = [
            ArrayQueryParameter("target_paths_chunk", "STRING", chunk)
        ]
        result = {
            row["target_path"] for row in bigquery_service.run_query(
                query, target_hca_dataset.project_id, target_hca_dataset.bq_location, query_params)
        }
        previously_loaded.update(result)

    return previously_loaded


def _get_credentials() -> Credentials:
    creds = get_credentials()
    creds.refresh(Request())
    return creds


def _fetch_drs_access_info(drs_host: str, drs_object_id: str, session: requests.Session) -> str:
    # direct call via python requests as the jade client throws a validation error on an empty/null
    # "self_uri" field in all responses
    drs_url = f"https://{drs_host}/ga4gh/drs/v1/objects/{drs_object_id}?expand=false"
    response = session.get(drs_url).json()
    for method in response['access_methods']:
        if method['type'] == 'gs':
            return str(method['access_url']['url'])
    raise Exception("No GS access method found")


def _extract_entities_to_path(
        nodes: dict[MetadataType, list[MetadataEntity]],
        destination_path: str,
        bigquery_project_id: str,
        snapshot_name: str,
        bigquery_region: str,
        bigquery_service: BigQueryService,
        chunk_size: int = BQ_CHUNK_SIZE
) -> None:
    for entity_type, entities in nodes.items():

        chunked_entities = chunked(entities, chunk_size)
        for i, chunk in enumerate(chunked_entities):
            data_file = False
            if entity_type.endswith("_file"):
                data_file = True

            if data_file:
                fetch_entities_query = f"""
                    EXPORT DATA OPTIONS(
                      uri='gs://{destination_path}/{entity_type}/{entity_type}_chunk_{i}_*.json',
                      format='JSON',
                      overwrite=true
                    ) AS
                    SELECT * EXCEPT (datarepo_row_id, file_id)
                    FROM `{bigquery_project_id}.{snapshot_name}.{entity_type}` WHERE {entity_type}_id IN
                    UNNEST(@entity_ids)
                """
            else:
                fetch_entities_query = f"""
                    EXPORT DATA OPTIONS(
                      uri='gs://{destination_path}/{entity_type}/{entity_type}_chunk_{i}_*.json',
                      format='JSON',
                      overwrite=true
                    ) AS
                    SELECT * EXCEPT (datarepo_row_id)
                    FROM `{bigquery_project_id}.{snapshot_name}.{entity_type}` WHERE {entity_type}_id IN
                    UNNEST(@entity_ids)
                    """
            entity_ids = [entity.entity_id for entity in chunk]
            query_params = [
                ArrayQueryParameter("entity_ids", "STRING", entity_ids)
            ]
            logging.info(f"Saved tabular data for ingest to gs://{destination_path}/{entity_type}/*")
            bigquery_service.run_query(
                fetch_entities_query, bigquery_project_id, bigquery_region, query_params)


def _fetch_file_entities(
        entities_by_type: dict[MetadataType, list[MetadataEntity]],
        bigquery_project_id: str,
        snapshot_name: str,
        bigquery_region: str,
        bigquery_service: BigQueryService,
        chunk_size: int = BQ_CHUNK_SIZE
) -> dict[str, list[Row]]:
    result: dict[str, list[Row]] = defaultdict(list[Row])
    for entity_type, entities in entities_by_type.items():
        if not entity_type.endswith("_file"):
            continue

        fetch_entities_query = f"""
        SELECT '/v1/' || JSON_EXTRACT_SCALAR(descriptor, '$.file_id') || '/' || JSON_EXTRACT_SCALAR(descriptor, '$.file_name') as target_path, * EXCEPT (datarepo_row_id)
        FROM `{bigquery_project_id}.{snapshot_name}.{entity_type}` WHERE {entity_type}_id IN
        UNNEST(@entity_ids)
        """

        chunked_entity_ids = chunked([entity.entity_id for entity in entities], chunk_size)
        for entity_ids in chunked_entity_ids:
            query_params = [
                ArrayQueryParameter("entity_ids", "STRING", entity_ids)
            ]
            result[entity_type] += [row for row in bigquery_service.run_query(fetch_entities_query,
                                                                              bigquery_project_id,
                                                                              bigquery_region,
                                                                              query_params)]

    return result
