from dagster import solid, ResourceDefinition
from dagster.core.execution.context.compute import (
    AbstractComputeExecutionContext,
)


@solid(
    required_resource_keys={"snapshot_config", "bigquery_service", "gcs"}
)
def ingest_tabular_data(context: AbstractComputeExecutionContext, scratch_bucket_name: str) -> str:
    snapshot_config = context.resources.snapshot_config
    bigquery_service = context.resources.bigquery_service
    gcs = context.resources.gcs

    return "scratch_bucket_name"
