import re

from dagster import solid, InputDefinition, Nothing, String
from dagster.core.execution.context.compute import AbstractComputeExecutionContext


STAGING_BUCKET_CONFIG = {
    "staging_bucket_name": String,
    "staging_prefix_name": String,
}


@solid(
    required_resource_keys={"storage_client"},
    config_schema=STAGING_BUCKET_CONFIG,
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
    context.log.debug(f"--clear_staging_dir found {deletions_count} blobs to delete under {staging_prefix_name}")
    return deletions_count


@solid(
    required_resource_keys={"beam_runner"},
    config_schema={
        **STAGING_BUCKET_CONFIG,
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

    kebabified_output_prefix = re.sub(r"[^A-Za-z0-9]", "-",  prefix_name)

    context.resources.beam_runner.run(
        job_name=f"hca-stage-metadata-{kebabified_output_prefix}",
        input_prefix=context.solid_config["input_prefix"],
        output_prefix=f'gs://{bucket_name}/{prefix_name}'
    )


@solid(
    required_resource_keys={"data_repo_client"},
    input_defs=[InputDefinition("start", Nothing)]
)
def submit_file_ingest(context: AbstractComputeExecutionContext) -> Nothing:
    """
    This will submit a dataset for ingestion to the data repo
    TODO This is a noop for now
    """
    datasets = context.resources.data_repo_client.enumerate_datasets()
    context.log.debug(f"Enumerate found {datasets.total} datasets in the repo")
