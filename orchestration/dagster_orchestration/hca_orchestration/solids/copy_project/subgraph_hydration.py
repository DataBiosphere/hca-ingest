import json
import requests
from collections import defaultdict
from dataclasses import dataclass
from urllib.parse import urlparse

from dagster import solid, InputDefinition, Nothing
from dagster.core.execution.context.compute import (
    AbstractComputeExecutionContext,
)
from dagster_utils.contrib.google import get_credentials
from google.cloud.bigquery import ArrayQueryParameter
from google.auth.transport.requests import Request

from hca_orchestration.contrib.bigquery import BigQueryService
from hca_orchestration.resources.config.scratch import ScratchConfig
from hca_orchestration.resources.snaphot_config import SnapshotConfig


@dataclass
class MetadataEntity:
    entity_type: str
    entity_id: str


@solid(
    required_resource_keys={
        "snapshot_config",
        "bigquery_service",
        "hca_project_config",
        "scratch_config"
    },
    input_defs=[InputDefinition("start", Nothing)]
)
def hydrate_subgraphs(context: AbstractComputeExecutionContext) -> set[str]:
    # 1. given a project ID, query the links table for all rows associated with the project
    # 2. find all process entries assoc. with the links
    # 3. find all other entities assoc. with the links
    snapshot_config: SnapshotConfig = context.resources.snapshot_config
    bigquery_service: BigQueryService = context.resources.bigquery_service
    hca_project_config = context.resources.hca_project_config
    project_id = hca_project_config.project_id
    scratch_config: ScratchConfig = context.resources.scratch_config

    scratch_bucket_name = f"{scratch_config.scratch_bucket_name}/{scratch_config.scratch_prefix_name}"

    query = f"""
      SELECT *
        FROM {snapshot_config.bigquery_project_id}.{snapshot_config.snapshot_name}.links
        WHERE project_id = "{project_id}"
    """
    query_job = bigquery_service.build_query_job_returning_data(query, snapshot_config.bigquery_project_id)
    subgraphs = [json.loads(row["content"])["links"] for row in query_job.result()]

    context.log.info("Hydrating subgraphs...")
    nodes = defaultdict(list)
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
        snapshot_config.bigquery_project_id,
        snapshot_config.snapshot_name,
        bigquery_service
    )

    context.log.info("Determining files to load...")
    entity_rows = fetch_entities(nodes,
                                 snapshot_config.bigquery_project_id,
                                 snapshot_config.snapshot_name,
                                 bigquery_service)

    drs_objects = {}
    for entity_type, entities in entity_rows.items():
        if not entity_type.endswith("_file"):
            continue
        for entity in entities:
            file_id = entity['file_id']
            drs_object = urlparse(file_id).path[1:]
            drs_objects[drs_object] = urlparse(file_id).netloc

    # get the actual GS path for the DRS object
    context.log.info("Resolving DRS object GCS paths...")
    paths = set()
    with requests.Session() as s:
        creds = _get_credentials()
        s.headers = {
            "Authorization": f"Bearer {creds.token}"
        }
        [
            paths.add(_fetch_drs_access_info(drs_host, drs_object, s))
            for drs_object, drs_host in drs_objects.items()
        ]

    return paths


def _get_credentials():
    creds = get_credentials()
    creds.refresh(Request())
    return creds


def _fetch_drs_access_info(drs_host: str, drs_object: str, session: requests.Session):
    # direct call via python requests as the jade client throws a validation error on an empty/null
    # "self_uri" field in all responses
    drs_url = f"https://{drs_host}/ga4gh/drs/v1/objects/{drs_object}?expand=false"
    response = session.get(drs_url).json()
    for method in response['access_methods']:
        if method['type'] == 'gs':
            return method['access_url']['url']
    raise Exception("No GS access method found")


def _extract_entities_to_path(
        nodes: dict[str, list[MetadataEntity]],
        destination_path: str,
        bigquery_project_id: str,
        snapshot_name: str,
        bigquery_service: BigQueryService
):
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
                FROM {bigquery_project_id}.{snapshot_name}.{entity_type} WHERE {entity_type}_id IN
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
                FROM {bigquery_project_id}.{snapshot_name}.{entity_type} WHERE {entity_type}_id IN
                UNNEST(@entity_ids)
                """
        entity_ids = [entity.entity_id for entity in entities]
        query_params = [
            ArrayQueryParameter("entity_ids", "STRING", entity_ids)
        ]
        print(f"**** Saved tabular data ingest to gs://{destination_path}/{entity_type}/*")
        query_job = bigquery_service.build_query_job_returning_data(
            fetch_entities_query, bigquery_project_id, query_params)
        query_job.result()


def fetch_entities(
        entities_by_type: dict[str, list],
        bigquery_project_id: str,
        snapshot_name: str,
        bigquery_service: BigQueryService):
    result = defaultdict(list)
    for entity_type, entities in entities_by_type.items():
        fetch_entities_query = f"""
        SELECT * EXCEPT (datarepo_row_id)
        FROM {bigquery_project_id}.{snapshot_name}.{entity_type} WHERE {entity_type}_id IN
        UNNEST(@entity_ids)
        """
        entity_ids = [entity.entity_id for entity in entities]
        query_params = [
            ArrayQueryParameter("entity_ids", "STRING", entity_ids)
        ]
        result[entity_type] = [row for row in bigquery_service.build_query_job_returning_data(fetch_entities_query,
                                                                                              bigquery_project_id,
                                                                                              query_params)]

    return result
