import re

from dagster import solid, InputDefinition, Nothing, String, Int, OutputDefinition
from dagster.core.execution.context.compute import AbstractComputeExecutionContext

from google.cloud.bigquery import Dataset


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


@solid(
    required_resource_keys={"bigquery_client", "load_tag"},
    config_schema={
        "staging_bq_project": String,
        "staging_dataset_prefix": String,
        "staging_table_expiration_ms": Int
    },
    input_defs=[InputDefinition("start", Nothing)],
    output_defs=[OutputDefinition(name="staging_dataset_name", dagster_type=String)]
)
def create_staging_dataset(context: AbstractComputeExecutionContext) -> str:
    """
    Creates a staging dataset that will house records for update/insertion into the
    final TDR dataset
    :return: Name of the staging dataset
    """
    staging_bq_project = context.solid_config["staging_bq_project"]
    staging_dataset_prefix = context.solid_config["staging_dataset_prefix"]
    load_tag = context.resources.load_tag

    dataset_name = f"{staging_bq_project}.{staging_dataset_prefix}_{load_tag}"

    dataset = Dataset(dataset_name)
    dataset.default_table_expiration_ms = context.solid_config["staging_table_expiration_ms"]

    bq_client = context.resources.bigquery_client
    bq_client.create_dataset(dataset)

    context.log.info(f"Created staging dataset {dataset_name}")

    return dataset_name
