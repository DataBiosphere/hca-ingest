from dagster import solid, ResourceDefinition
from dagster.core.execution.context.compute import (
    AbstractComputeExecutionContext,
)


@solid(
    required_resource_keys={"snapshot_config", "bigquery_service", "gcs", "data_repo_client"}
)
def create_data_file_copy(context: AbstractComputeExecutionContext, scratch_bucket_name: str):
    snapshot_config = context.resources.snapshot_config
    bigquery_service = context.resources.bigquery_service
    gcs = context.resources.gcs
    data_repo_client = context.resources.data_repo_client
    return "scratch_bucket_name"