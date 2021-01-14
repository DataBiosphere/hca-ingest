from dagster import solid, Nothing, InputDefinition, ExpectationResult, String
import google.auth
from google.cloud import storage


STAGING_BUCKET_NAME = "staging_bucket_name"
STAGING_BLOB_NAME = "staging_blob_name"


@solid(
    config_schema={
        STAGING_BUCKET_NAME: str,
        STAGING_BLOB_NAME: str
    }
)
def clear_staging_dir(context) -> Nothing:
    bucket_name = context.solid_config[STAGING_BUCKET_NAME]
    blob_name = context.solid_config[STAGING_BLOB_NAME]

    credentials, project = google.auth.default()

    storage_client = storage.Client(project=project, credentials=credentials)

    blobs = storage_client.list_blobs(bucket_name, prefix=f"{blob_name}/")
    dels = 0
    for blob in blobs:
        blob.delete()
        dels += 1
    context.log.debug(f"--clear_staging_dir found {dels} blobs to delete under {blob_name}")



@solid(
    input_defs=[InputDefinition("start", Nothing)],
    config_schema={
        "input_prefix": String,
        "output_prefix": String

    },
    required_resource_keys={"beam_runner"}
)
def pre_process_metadata(context) -> Nothing:
    context.log.info(f"--pre_process_metadata")
    input_prefix = context.solid_config["input_prefix"]
    output_prefix = context.solid_config["output_prefix"]

    yield ExpectationResult(
        success=(input_prefix != output_prefix),
        label="input_prefix_ne_output_prefix",
        description="Check that input prefix differs from output prefix"
    )
    context.resources.beam_runner.run("pre-process-metadata", input_prefix, output_prefix, context)
