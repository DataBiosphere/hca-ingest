from dagster import solid, Nothing, String, DagsterType
from hca_utils.utils import HcaUtils, ProblemCount

DagsterProblemCount = DagsterType(
    name="DagsterProblemCount",
    type_check_fn=lambda _, value: isinstance(value, ProblemCount),
    description="A simple named tuple to represent the different types of issues present from the post process validation.",
)


@solid(
    required_resource_keys={"storage_client"}
)
def clear_staging_dir(context, staging_bucket_name: str, staging_prefix_name: str) -> int:
    """
    Given a staging bucket + prefix, deletes all blobs present at that path
    :return: Number of deletions
    """

    blobs = context.resources.storage_client.list_blobs(staging_bucket_name, prefix=f"{staging_prefix_name}/")
    deletions_count = 0
    for blob in blobs:
        blob.delete()
        deletions_count += 1
    context.log.debug(f"--clear_staging_dir found {deletions_count} blobs to delete under {staging_prefix_name}")
    return deletions_count


@solid(
    required_resource_keys={"beam_runner"}
)
def pre_process_metadata(context, input_prefix: str, output_prefix: str) -> Nothing:
    """
    Runs the Beam hca transformation pipeline flow over the given input prefix
    """
    context.log.info("--pre_process_metadata")

    context.resources.beam_runner.run("pre-process-metadata", input_prefix, output_prefix, context)


@solid(
    required_resource_keys={"data_repo_client"}
)
def submit_file_ingest(context) -> Nothing:
    """
    This will submit a dataset for ingestion to the data repo
    TODO This is a noop for now
    """
    datasets = context.resources.data_repo_client.enumerate_datasets()
    context.log.debug(f"Enumerate found {datasets.total} datasets in the repo")


@solid(
    config_schema={"gcp_env": String}
)
def post_import_validate(context, google_project_name: str, dataset_name: str) -> DagsterProblemCount:
    """
    Checks if the target dataset has any rows with duplicate IDs or null file references.
    """
    validator = HcaUtils(context.solid_config["gcp_env"], google_project_name, dataset_name)
    return validator.check_for_all()
