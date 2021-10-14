import logging
from collections import defaultdict
from urllib.parse import urlparse

import requests
from dagster import Nothing, op, In
from dagster.core.execution.context.compute import (
    AbstractComputeExecutionContext,
)
from dagster_utils.contrib.google import get_credentials
from google.auth.transport.requests import Request
from google.cloud.bigquery import ArrayQueryParameter, Row
from google.oauth2.credentials import Credentials
from requests.structures import CaseInsensitiveDict

from hca_orchestration.contrib.bigquery import BigQueryService
from hca_orchestration.models.entities import DataFileEntity, MetadataEntity, build_subgraph_from_links_row
from hca_orchestration.models.scratch import ScratchConfig
from hca_orchestration.resources.hca_project_config import HcaProjectCopyingConfig
from hca_orchestration.support.subgraphs import build_subgraph_nodes
from hca_orchestration.support.typing import MetadataType


@op(
    required_resource_keys={
        "bigquery_service",
        "hca_project_copying_config",
        "scratch_config"
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

    scratch_bucket_name = f"{scratch_config.scratch_bucket_name}/{scratch_config.scratch_prefix_name}"

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
    _extract_entities_to_path(
        nodes,
        f"{scratch_bucket_name}/tabular_data_for_ingest",
        hca_project_config.source_bigquery_project_id,
        hca_project_config.source_snapshot_name,
        hca_project_config.source_bigquery_region,
        bigquery_service
    )

    context.log.info("Determining files to load...")

    entity_rows: dict[str, list[Row]] = fetch_entities(nodes,
                                                       hca_project_config.source_bigquery_project_id,
                                                       hca_project_config.source_snapshot_name,
                                                       hca_project_config.source_bigquery_region,
                                                       bigquery_service)

    drs_objects = {}
    entity_file_ids = {}
    for entity_type, rows in entity_rows.items():
        if not entity_type.endswith("_file"):
            continue
        for row in rows:
            tdr_file_id = row['file_id']
            drs_object = urlparse(tdr_file_id).path[1:]
            drs_objects[drs_object] = urlparse(tdr_file_id).netloc

            entity_file_ids[drs_object] = row[f"target_path"]

    # get the actual GS path for the DRS object
    context.log.info(f"Resolving DRS object GCS paths ({len(drs_objects)} objects)...")

    data_entities = set()
    with requests.Session() as s:
        creds = _get_credentials()
        s.headers = CaseInsensitiveDict[str]()
        s.headers["Authorization"] = f"Bearer {creds.token}"

        for cnt, (drs_object, drs_host) in enumerate(drs_objects.items(), start=1):
            if creds.expired:
                context.log.info("Refreshing expired credentials")
                creds = _get_credentials()
                s.headers["Authorization"] = f"Bearer {creds.token}"

            if cnt % 100 == 0:
                context.log.info(f"Resolved {cnt} paths...")

            data_entities.add(
                DataFileEntity(
                    _fetch_drs_access_info(
                        drs_host,
                        drs_object,
                        s),
                    entity_file_ids[drs_object]))
        context.log.info(f"Resolved {len(data_entities)} total paths")

    return data_entities


def _get_credentials() -> Credentials:
    creds = get_credentials()
    creds.refresh(Request())
    return creds


def _fetch_drs_access_info(drs_host: str, drs_object: str, session: requests.Session) -> str:
    # direct call via python requests as the jade client throws a validation error on an empty/null
    # "self_uri" field in all responses
    drs_url = f"https://{drs_host}/ga4gh/drs/v1/objects/{drs_object}?expand=false"
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
        bigquery_service: BigQueryService
) -> None:
    for entity_type, entities in nodes.items():
        data_file = False
        if entity_type.endswith("_file"):
            data_file = True

        if data_file:
            fetch_entities_query = f"""
                EXPORT DATA OPTIONS(
                  uri='gs://{destination_path}/{entity_type}/*',
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
                  uri='gs://{destination_path}/{entity_type}/*',
                  format='JSON',
                  overwrite=true
                ) AS
                SELECT * EXCEPT (datarepo_row_id)
                FROM `{bigquery_project_id}.{snapshot_name}.{entity_type}` WHERE {entity_type}_id IN
                UNNEST(@entity_ids)
                """
        entity_ids = [entity.entity_id for entity in entities]
        query_params = [
            ArrayQueryParameter("entity_ids", "STRING", entity_ids)
        ]
        logging.info(f"Saved tabular data for ingest to gs://{destination_path}/{entity_type}/*")
        bigquery_service.run_query(
            fetch_entities_query, bigquery_project_id, bigquery_region, query_params)


def fetch_entities(
        entities_by_type: dict[MetadataType, list[MetadataEntity]],
        bigquery_project_id: str,
        snapshot_name: str,
        bigquery_region: str,
        bigquery_service: BigQueryService) -> dict[str, list[Row]]:
    result: dict[str, list[Row]] = defaultdict(list[Row])
    for entity_type, entities in entities_by_type.items():
        if not entity_type.endswith("_file"):
            fetch_entities_query = f"""
            SELECT * EXCEPT (datarepo_row_id)
            FROM `{bigquery_project_id}.{snapshot_name}.{entity_type}` WHERE {entity_type}_id IN
            UNNEST(@entity_ids)
            """
        else:
            fetch_entities_query = f"""
            SELECT '/' || JSON_EXTRACT_SCALAR(descriptor, '$.file_id') || '/' || JSON_EXTRACT_SCALAR(descriptor, '$.file_name') as target_path, * EXCEPT (datarepo_row_id)
            FROM `{bigquery_project_id}.{snapshot_name}.{entity_type}` WHERE {entity_type}_id IN
            UNNEST(@entity_ids)
            """
        entity_ids = [entity.entity_id for entity in entities]
        query_params = [
            ArrayQueryParameter("entity_ids", "STRING", entity_ids)
        ]
        result[entity_type] = [row for row in bigquery_service.run_query(fetch_entities_query,
                                                                         bigquery_project_id,
                                                                         bigquery_region,
                                                                         query_params)]

    return result
