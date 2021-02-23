from dagster import solid, Nothing, InputDefinition, String

STAGING_BUCKET_NAME = "staging_bucket_name"
STAGING_PREFIX_NAME = "staging_prefix_name"


@solid(
    config_schema={
        STAGING_BUCKET_NAME: str,
        STAGING_PREFIX_NAME: str
    },
    required_resource_keys={"storage_client"}
)
def clear_staging_dir(context) -> int:
    """
    Given a staging bucket + prefix, deletes all blobs present at that path
    :return: Number of deletions
    """
    bucket_name = context.solid_config[STAGING_BUCKET_NAME]
    prefix_name = context.solid_config[STAGING_PREFIX_NAME]

    blobs = context.resources.storage_client.list_blobs(bucket_name, prefix=f"{prefix_name}/")
    dels = 0
    for blob in blobs:
        blob.delete()
        dels += 1
    context.log.debug(f"--clear_staging_dir found {dels} blobs to delete under {prefix_name}")
    return dels


@solid(
    input_defs=[InputDefinition("start", Nothing)],
    config_schema={
        "input_prefix": String,
        "output_prefix": String

    },
    required_resource_keys={"beam_runner"}
)
def pre_process_metadata(context) -> Nothing:
    """
    Runs the Beam hca transformation pipeline flow over the given input prefix
    """
    context.log.info(f"--pre_process_metadata")
    input_prefix = context.solid_config["input_prefix"]
    output_prefix = context.solid_config["output_prefix"]

    context.resources.beam_runner.run("pre-process-metadata", input_prefix, output_prefix, context)


@solid(
    input_defs=[InputDefinition("start", Nothing)],
    required_resource_keys={"data_repo_client"}
)
def submit_file_ingest(context) -> Nothing:
    """
    This will submit a dataset for ingestion to the data repo
    TODO This is a noop for now
    """
    datasets = context.resources.data_repo_client.enumerate_datasets()
    context.log.debug(f"Enumerate found {datasets.total} datasets in the repo")
