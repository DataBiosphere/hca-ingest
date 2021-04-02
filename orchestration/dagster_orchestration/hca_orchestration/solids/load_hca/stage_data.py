import re
import uuid

from dagster import composite_solid, solid, InputDefinition, Nothing, String, OutputDefinition
from dagster.experimental import DynamicOutput, DynamicOutputDefinition
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from google.cloud import bigquery

STAGING_BUCKET_CONFIG_SCHEMA = {
    "staging_bucket_name": String,
    "staging_prefix_name": String,
}


@solid(
    required_resource_keys={"storage_client"},
    config_schema=STAGING_BUCKET_CONFIG_SCHEMA,
)
def clear_staging_dir(context: AbstractComputeExecutionContext) -> int:
    """
    Given a staging bucket + prefix, deletes all blobs present at that path
    :return: Number of deletions
    """

    staging_bucket_name = context.solid_config["staging_bucket_name"]
    staging_prefix_name = context.solid_config["staging_prefix_name"]

    blobs = context.resources.storage_client.list_blobs(staging_bucket_name, prefix=f"{staging_prefix_name}/")
    deletions_count = 0
    for blob in blobs:
        blob.delete()
        deletions_count += 1
    context.log.debug(f"--clear_staging_dir deleted {deletions_count} blobs under {staging_prefix_name}")
    return deletions_count


@solid(
    required_resource_keys={"beam_runner"},
    config_schema={
        **STAGING_BUCKET_CONFIG_SCHEMA,
        "input_prefix": String,
    },
    input_defs=[InputDefinition("start", Nothing)],
)
def pre_process_metadata(context: AbstractComputeExecutionContext) -> Nothing:
    """
    Runs the Beam hca transformation pipeline flow over the given input prefix
    """
    context.log.info("--pre_process_metadata")

    # not strictly required, but makes the ensuing lines a lot shorter
    bucket_name = context.solid_config['staging_bucket_name']
    prefix_name = context.solid_config['staging_prefix_name']

    kebabified_output_prefix = re.sub(r"[^A-Za-z0-9]", "-", prefix_name)

    context.resources.beam_runner.run(
        job_name=f"hca-stage-metadata-{kebabified_output_prefix}",
        input_prefix=context.solid_config["input_prefix"],
        output_prefix=f'gs://{bucket_name}/{prefix_name}'
    )


def bigquery_client(project) -> bigquery.client.Client:
    return bigquery.Client(project=project)


@solid(
    config_schema={
        "staging_bq_project": String,
    },
    input_defs=[InputDefinition("start", Nothing)],
    output_defs=[OutputDefinition(name="staging_dataset_name", dagster_type=str)]
)
def create_staging_dataset(context: AbstractComputeExecutionContext) -> str:
    staging_bq_project = context.solid_config['staging_bq_project']

    # TODO config out this prefix
    dataset_name = f"{staging_bq_project}.arh_staging_test_{uuid.uuid4().hex[:8]}"

    dataset = bigquery.Dataset(dataset_name)

    # TODO (low-pri) config out the TTL
    dataset.default_table_expiration_ms = 3600000
    client = bigquery_client(project=staging_bq_project)
    client.create_dataset(dataset)

    return dataset_name
