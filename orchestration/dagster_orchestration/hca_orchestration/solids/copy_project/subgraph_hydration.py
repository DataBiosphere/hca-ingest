import json
from collections import defaultdict
from urllib.parse import urlparse

import requests
from dagster import InputDefinition, Nothing, op
from dagster.core.execution.context.compute import (
    AbstractComputeExecutionContext,
)
from dagster_utils.contrib.google import get_credentials
from google.auth.transport.requests import Request
from google.cloud.bigquery import ArrayQueryParameter, Row
from google.oauth2.credentials import Credentials
from requests.structures import CaseInsensitiveDict

from hca_orchestration.contrib.bigquery import BigQueryService
from hca_orchestration.models.entities import DataFileEntity, MetadataEntity
from hca_orchestration.models.scratch import ScratchConfig
from hca_orchestration.resources.hca_project_config import HcaProjectCopyingConfig
from hca_orchestration.support.typing import MetadataType


@op(
    required_resource_keys={
        "bigquery_service",
        "hca_project_copying_config",
        "scratch_config"
    },
    input_defs=[InputDefinition("start", Nothing)]
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
    rows = bigquery_service.run_query(query, hca_project_config.source_bigquery_project_id)
    nodes = defaultdict(list)
    subgraphs = []
    for row in rows:
        subgraphs.append(json.loads(row["content"])["links"])
        nodes["links"].append(MetadataEntity(MetadataType("link"), row["links_id"]))

    context.log.info("Hydrating subgraphs...")
    for subgraph in subgraphs:

        for link in subgraph:
            link_type = link["link_type"]
            if link_type == 'process_link':
                process = MetadataEntity(link["process_type"], link["process_id"])
                nodes[process.entity_type].append(process)

                for input_link in link["inputs"]:
                    input_entity = MetadataEntity(input_link["input_type"], input_link["input_id"])
                    nodes[input_entity.entity_type].append(input_entity)

                for output_link in link["outputs"]:
                    output_entity = MetadataEntity(output_link["output_type"], output_link["output_id"])
                    nodes[output_entity.entity_type].append(output_entity)

                for protocol_link in link["protocols"]:
                    protocol_entity = MetadataEntity(protocol_link["protocol_type"], protocol_link["protocol_id"])
                    nodes[protocol_entity.entity_type].append(protocol_entity)

            elif link_type == 'supplementary_file_link':
                entity = MetadataEntity(link["entity"]["entity_type"], link["entity"]["entity_id"])
                nodes[entity.entity_type].append(entity)

                for file_link in link['files']:
                    file_entity = MetadataEntity(file_link["file_type"], file_link["file_id"])
                    nodes[file_entity.entity_type].append(file_entity)
            else:
                raise Exception(f"Unknown link type {link_type} encountered")

    context.log.info("Saving entities to extract scratch path...")
    _extract_entities_to_path(
        nodes,
        f"{scratch_bucket_name}/tabular_data_for_ingest",
        hca_project_config.source_bigquery_project_id,
        hca_project_config.source_snapshot_name,
        bigquery_service
    )

    context.log.info("Determining files to load...")
    entity_rows: dict[str, list[Row]] = fetch_entities(nodes,
                                                       hca_project_config.source_bigquery_project_id,
                                                       hca_project_config.source_snapshot_name,
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
    context.log.info("Resolving DRS object GCS paths...")
    data_entities = set()
    with requests.Session() as s:
        creds = _get_credentials()
        s.headers = CaseInsensitiveDict[str]()
        s.headers["Authorization"] = f"Bearer {creds.token}"

        for drs_object, drs_host in drs_objects.items():
            data_entities.add(
                DataFileEntity(
                    _fetch_drs_access_info(
                        drs_host,
                        drs_object,
                        s),
                    entity_file_ids[drs_object]))

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
        nodes: dict[str, list[MetadataEntity]],
        destination_path: str,
        bigquery_project_id: str,
        snapshot_name: str,
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
        print(f"**** Saved tabular data ingest to gs://{destination_path}/{entity_type}/*")
        bigquery_service.run_query(
            fetch_entities_query, bigquery_project_id, query_params, location='US')


def fetch_entities(
        entities_by_type: dict[str, list[MetadataEntity]],
        bigquery_project_id: str,
        snapshot_name: str,
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
                                                                         query_params)]

    return result
