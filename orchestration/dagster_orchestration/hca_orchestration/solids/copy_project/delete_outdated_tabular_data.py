from dagster import solid, InputDefinition
from dagster.core.execution.context.compute import (
    AbstractComputeExecutionContext,
)
from dagster_utils.contrib.data_repo.jobs import poll_job
from dagster_utils.contrib.data_repo.typing import JobId
from data_repo_client import JobModel

from hca_orchestration.contrib.bigquery import BigQueryService
from hca_orchestration.resources.config.hca_dataset import TargetHcaDataset
from hca_orchestration.resources.config.scratch import ScratchConfig


@solid(
    required_resource_keys={"bigquery_service", "target_hca_dataset", "scratch_config", "data_repo_client"},
    input_defs=[InputDefinition("entity_types", set[str])]
)
def delete_outdated_tabular_data(context: AbstractComputeExecutionContext, entity_types: set[str]) -> None:
    """Soft-deletes outdated and duplicate data in each entity type table"""

    target_hca_dataset: TargetHcaDataset = context.resources.target_hca_dataset
    bigquery_service: BigQueryService = context.resources.bigquery_service
    scratch_config: ScratchConfig = context.resources.scratch_config
    data_repo_client = context.resources.data_repo_client

    destination_path = f"{scratch_config.scratch_bucket_name}/{scratch_config.scratch_prefix_name}"
    for entity_type in entity_types:
        dupes_path = f"gs://{destination_path}/outdated_row_ids/{entity_type}/*"
        _extract_dupes_to_scratch_area(bigquery_service, dupes_path, entity_type, target_hca_dataset)
        payload = {
            "deleteType": "soft",
            "specType": "gcsFile",
            "tables": [
                {
                    "gcsFileSpec": {
                        "fileType": "csv",
                        "path": dupes_path
                    },
                    "tableName": entity_type
                }
            ]
        }

        context.log.info(f"Submitting soft deletes for {entity_type}...")
        job_response: JobModel = data_repo_client.apply_dataset_data_deletion(
            id=target_hca_dataset.dataset_id,
            data_deletion_request=payload
        )

        job_id = JobId(job_response.id)
        context.log.info(f"Soft deletes submitted, polling on job_id = {job_id}")
        poll_job(job_id, 240, 2, data_repo_client)


def _extract_dupes_to_scratch_area(bigquery_service, dupes_path, entity_type, target_hca_dataset):
    """
    For all rows in a given HCA table, returns all but the latest version; in the event of duplicate
    latest versions, returns the duplicates as well
    """
    query = f"""
            EXPORT DATA OPTIONS(
                uri='{dupes_path}',
                format='CSV',
                overwrite=true
            ) AS
            WITH rows_ordered_by_version AS (
                SELECT datarepo_row_id, {entity_type}_id, version, rank() OVER (
                    PARTITION BY {entity_type}_id ORDER BY version ASC, datarepo_row_id ASC
                ) AS rank
                FROM `{target_hca_dataset.source_hca_project_id}.datarepo_{target_hca_dataset.dataset_name}.{entity_type}`
                          ORDER BY {entity_type}_id
            )
            SELECT datarepo_row_id FROM rows_ordered_by_version WHERE rank > 1;
        """
    query_job = bigquery_service.build_query_job(
        query, target_hca_dataset.source_hca_project_id, location='us-central1')
    query_job.result()
