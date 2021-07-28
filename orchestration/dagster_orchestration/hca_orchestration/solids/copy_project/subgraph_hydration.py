import json
from collections import defaultdict
from dataclasses import dataclass

from dagster import solid, ResourceDefinition
from dagster.core.execution.context.compute import (
    AbstractComputeExecutionContext,
)
from google.cloud.bigquery import ArrayQueryParameter

from hca_orchestration.contrib.bigquery import BigQueryService
from hca_orchestration.resources.snaphot_config import SnapshotConfig


@dataclass
class MetadataEntity:
    entity_type: str
    entity_id: str


@solid(
    required_resource_keys={
        "snapshot_config",
        "bigquery_service",
        "hca_project_config"
    }
)
def hydrate_subgraphs(context: AbstractComputeExecutionContext, scratch_bucket_name: str) -> str:
    # 1. given a project ID, query the links table for all rows associated with the project
    # 2. find all process entries assoc. with the links
    # 3. find all other entities assoc. with the links
    snapshot_config: SnapshotConfig = context.resources.snapshot_config
    bigquery_service: BigQueryService = context.resources.bigquery_service
    hca_project_config = context.resources.hca_project_config
    project_id = hca_project_config.project_id

    query = f"""
      SELECT *
        FROM {snapshot_config.bigquery_project_id}.{snapshot_config.snapshot_name}.links
        WHERE project_id = "{project_id}"
    """
    query_job = bigquery_service.build_query_job_returning_data(query, snapshot_config.bigquery_project_id)
    subgraphs = [json.loads(row["content"])["links"] for row in query_job.result()]

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

    _extract_entities_to_path(
        nodes,
        f"{scratch_bucket_name}/tabular_data_for_ingest",
        snapshot_config.bigquery_project_id,
        snapshot_config.snapshot_name,
        bigquery_service
    )
    return scratch_bucket_name


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
                  uri='{destination_path}/{entity_type}/*',
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
                  uri='{destination_path}/{entity_type}/*',
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
        query_job = bigquery_service.build_query_job_returning_data(fetch_entities_query, query_params)
        query_job.result()
