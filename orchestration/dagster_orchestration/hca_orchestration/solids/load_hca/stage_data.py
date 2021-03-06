import re

from dagster import solid, InputDefinition, Nothing, String, Int
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from google.cloud.bigquery import Dataset

from hca_orchestration.support.typing import HcaStagingDatasetName


@solid(
    required_resource_keys={"storage_client", "scratch_config"},
)
def clear_scratch_dir(context: AbstractComputeExecutionContext) -> int:
    """
    Given a staging bucket + prefix, deletes all blobs present at that path
    :return: Number of deletions
    """

    scratch_bucket_name = context.resources.scratch_config.scratch_bucket_name
    scratch_prefix_name = context.resources.scratch_config.scratch_prefix_name

    blobs = context.resources.storage_client.list_blobs(scratch_bucket_name, prefix=f"{scratch_prefix_name}/")
    deletions_count = 0
    for blob in blobs:
        blob.delete()
        deletions_count += 1
    context.log.debug(f"--clear_scratch_dir deleted {deletions_count} blobs under {scratch_prefix_name}")
    return deletions_count


@solid(
    required_resource_keys={"beam_runner", "scratch_config"},
    config_schema={
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
    bucket_name = context.resources.scratch_config.scratch_bucket_name
    prefix_name = context.resources.scratch_config.scratch_prefix_name

    kebabified_output_prefix = re.sub(r"[^A-Za-z0-9]", "-", prefix_name)

    context.resources.beam_runner.run(
        job_name=f"hca-stage-metadata-{kebabified_output_prefix}",
        input_prefix=context.solid_config["input_prefix"],
        output_prefix=f'gs://{bucket_name}/{prefix_name}'
    )


@solid(
    required_resource_keys={"bigquery_client", "load_tag", "scratch_config"},

    input_defs=[InputDefinition("start", Nothing)],
)
def create_scratch_dataset(context: AbstractComputeExecutionContext) -> HcaStagingDatasetName:
    """
    Creates a staging dataset that will house records for update/insertion into the
    final TDR dataset
    :return: Name of the staging dataset
    """
    scratch_bq_project = context.resources.scratch_config.scratch_bq_project
    scratch_dataset_prefix = context.resources.scratch_config.scratch_dataset_prefix
    load_tag = context.resources.load_tag

    dataset_name = f"{scratch_bq_project}.{scratch_dataset_prefix}_{load_tag}"

    dataset = Dataset(dataset_name)
    dataset.default_table_expiration_ms = context.resources.scratch_config.scratch_table_expiration_ms

    bq_client = context.resources.bigquery_client
    bq_client.create_dataset(dataset)

    context.log.info(f"Created scratch dataset {dataset_name}")

    return HcaStagingDatasetName(dataset_name)
