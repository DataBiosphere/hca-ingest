from dagster import solid, ResourceDefinition, Nothing, InputDefinition
from dagster.core.execution.context.compute import (
    AbstractComputeExecutionContext,
)


@solid(
    required_resource_keys={"gcs", "data_repo_client"},
    input_defs=[InputDefinition("start", Nothing)]
)
def inject_file_ids(context: AbstractComputeExecutionContext) -> str:
    gcs = context.resources.gcs
    data_repo_client = context.resources.data_repo_client
    return "scratch_bucket_name"
