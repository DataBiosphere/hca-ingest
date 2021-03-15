from dagster import solid, InputDefinition, Nothing, String, DagsterType

from hca_manage.manage import HcaManage, ProblemCount

from typing import Any

POST_VALIDATION_SETTINGS_SCHEMA = {
    "gcp_env": String,
    "dataset_name": String,
}


def problem_count_typecheck(_, value: Any) -> bool:
    return isinstance(value, ProblemCount)


DagsterProblemCount: DagsterType = DagsterType(
    name="DagsterProblemCount",
    type_check_fn=problem_count_typecheck,
    description="A simple named tuple to represent the different types of issues "
                "present from the post process validation.",
)


@solid(
    required_resource_keys={"storage_client"},
    config_schema={
        "staging_bucket_name": String,
        "staging_prefix_name": String,
    },
)
def clear_staging_dir(context) -> int:
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
        "input_prefix": String,
        "output_prefix": String,
    },
    input_defs=[InputDefinition("start", Nothing)],
)
def pre_process_metadata(context) -> Nothing:
    """
    Runs the Beam hca transformation pipeline flow over the given input prefix
    """
    context.log.info("--pre_process_metadata")

    context.resources.beam_runner.run(
        "pre-process-metadata",
        context.solid_config["input_prefix"],
        context.solid_config["output_prefix"],
        context
    )


@solid(
    required_resource_keys={"data_repo_client"},
    input_defs=[InputDefinition("start", Nothing)]
)
def submit_file_ingest(context) -> Nothing:
    """
    This will submit a dataset for ingestion to the data repo
    TODO This is a noop for now
    """
    datasets = context.resources.data_repo_client.enumerate_datasets()
    context.log.debug(f"Enumerate found {datasets.total} datasets in the repo")


@solid(
    required_resource_keys={"data_repo_client"},
    config_schema={
        **POST_VALIDATION_SETTINGS_SCHEMA,
        "google_project_name": String,
    }
)
def post_import_validate(context) -> DagsterProblemCount:
    """
    Checks if the target dataset has any rows with duplicate IDs or null file references.
    """
    validator = HcaManage(
        environment=context.solid_config["gcp_env"],
        project=context.solid_config["google_project_name"],
        dataset=context.solid_config["dataset_name"],
        data_repo_client=context.resources.data_repo_client)
    return validator.check_for_all()


@solid(
    required_resource_keys={"slack"},
    input_defs=[InputDefinition("validation_results", DagsterProblemCount)],
    config_schema={
        **POST_VALIDATION_SETTINGS_SCHEMA,
        "channel": String,
    }
)
def notify_slack_of_egress_validation_results(context, validation_results) -> Nothing:
    if validation_results.duplicates > 0 or validation_results.null_file_refs > 0:
        gcp_env = context.solid_config["gcp_env"]
        dataset_name = context.solid_config["dataset_name"]
        message_lines = [
            f"Problems identified in post-validation for {gcp_env} dataset {dataset_name}:",
            "Duplicate lines found: " + validation_results.duplicates,
            "Null file references found: " + validation_results.null_file_refs,
        ]
    else:
        message_lines = [f"{gcp_env} dataset {dataset_name} has passed post-validation."]
    context.resources.slack.chat_postMessage(
        channel=context.solid_config["channel"],
        text="\n".join(message_lines))
