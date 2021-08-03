from dagster import solid, ResourceDefinition, Nothing, InputDefinition
from dagster.core.execution.context.compute import (
    AbstractComputeExecutionContext,
)


@solid(
    required_resource_keys={"snapshot_config", "bigquery_service", "gcs", "data_repo_client"},
    input_defs=[InputDefinition("start", Nothing)]
)
def copy_data_files(context: AbstractComputeExecutionContext) -> str:
    snapshot_config = context.resources.snapshot_config
    bigquery_service = context.resources.bigquery_service
    gcs = context.resources.gcs
    data_repo_client = context.resources.data_repo_client
    return "scratch_bucket_name"
