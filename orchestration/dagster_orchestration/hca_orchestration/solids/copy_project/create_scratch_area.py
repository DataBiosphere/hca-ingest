from dagster import solid, ResourceDefinition
from dagster.core.execution.context.compute import (
    AbstractComputeExecutionContext,
)


@solid(
    required_resource_keys={"snapshot_config", "gcs", "data_repo_client"}
)
def create_scratch_area(context: AbstractComputeExecutionContext):
    snapshot_config = context.resources.snapshot_config
    gcs = context.resources.gcs
    data_repo_client = context.resources.data_repo_client
    #Change return
    return "scratch_bucket_name"
