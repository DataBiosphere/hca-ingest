from dagster import solid, InputDefinition
from dagster.core.execution.context.compute import (
    AbstractComputeExecutionContext,
)
from dagster_utils.contrib.data_repo.jobs import poll_job
from dagster_utils.contrib.data_repo.typing import JobId
from dagster_utils.contrib.google import path_has_any_data, parse_gs_path
from data_repo_client import JobModel

from hca_orchestration.contrib.bigquery import BigQueryService
from hca_orchestration.models.hca_dataset import HcaDataset
from hca_orchestration.models.scratch import ScratchConfig


@solid(
    required_resource_keys={"bigquery_service", "target_hca_dataset", "scratch_config", "data_repo_client", "gcs"},
    input_defs=[InputDefinition("entity_types", set[str])]
)
def delete_outdated_tabular_data(context: AbstractComputeExecutionContext, entity_types: set[str]) -> None:
    """Soft-deletes outdated and duplicate data in each entity type table"""

    target_hca_dataset: HcaDataset = context.resources.target_hca_dataset
    bigquery_service: BigQueryService = context.resources.bigquery_service
    scratch_config: ScratchConfig = context.resources.scratch_config
    data_repo_client = context.resources.data_repo_client

    base_path = f"{scratch_config.scratch_bucket_name}/{scratch_config.scratch_prefix_name}"
    for entity_type in entity_types:
        destination_path = parse_gs_path(f"gs://{base_path}/outdated_row_ids/{entity_type}")
        query_job = bigquery_service.build_extract_duplicates_job(
            destination_path, entity_type, target_hca_dataset, 'us-central1')
        query_job.result()

        # todo clean up
        if not path_has_any_data(destination_path.bucket, destination_path.prefix, context.resources.gcs):
            context.log.info(f"Path {destination_path.to_gs_path()} has no soft deletes to submit, skipping...")
            continue

        payload = {
            "deleteType": "soft",
            "specType": "gcsFile",
            "tables": [
                {
                    "gcsFileSpec": {
                        "fileType": "csv",
                        "path": f"{destination_path.to_gs_path()}/*"
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
