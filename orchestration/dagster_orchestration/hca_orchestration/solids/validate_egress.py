import os
from typing import Any

from dagster import configured, solid, InputDefinition, Nothing, String, DagsterType

from hca_manage.manage import HcaManage, ProblemCount


def problem_count_typecheck(_, value: Any) -> bool:
    return isinstance(value, ProblemCount)


DagsterProblemCount: DagsterType = DagsterType(
    name="DagsterProblemCount",
    type_check_fn=problem_count_typecheck,
    description="A simple named tuple to represent the different types of issues "
                "present from the post process validation.",
)


POST_VALIDATION_SETTINGS_SCHEMA = {
    "gcp_env": String,
    "dataset_name": String,
}


@solid(
    required_resource_keys={"data_repo_client"},
    config_schema={
        **POST_VALIDATION_SETTINGS_SCHEMA,
        "google_project_name": String,
    }
)
def base_post_import_validate(context) -> DagsterProblemCount:
    """
    Checks if the target dataset has any rows with duplicate IDs or null file references.
    """
    validator = HcaManage(
        environment=context.solid_config["gcp_env"],
        project=context.solid_config["google_project_name"],
        dataset=context.solid_config["dataset_name"],
        data_repo_client=context.resources.data_repo_client)
    return validator.check_for_all()


# sets up default config settings to minimize pipeline boilerplate
@configured(base_post_import_validate, {"dataset_name": String})
def post_import_validate(config):
    return {
        'gcp_env': os.environ.get("HCA_GCP_ENV"),
        'google_project_name': os.environ.get("HCA_GOOGLE_PROJECT"),
        **config,
    }


@solid(
    required_resource_keys={"slack"},
    input_defs=[InputDefinition("validation_results", DagsterProblemCount)],
    config_schema={
        **POST_VALIDATION_SETTINGS_SCHEMA,
        "channel": String,
    }
)
def base_notify_slack_of_egress_validation_results(context, validation_results) -> Nothing:
    gcp_env = context.solid_config["gcp_env"]
    dataset_name = context.solid_config["dataset_name"]

    if validation_results.duplicates > 0 or validation_results.null_file_refs > 0:
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


@configured(base_notify_slack_of_egress_validation_results, {"dataset_name": String})
def notify_slack_of_egress_validation_results(config):
    return {
        'gcp_env': os.environ.get("HCA_GCP_ENV"),
        'channel': os.environ.get("SLACK_NOTIFICATIONS_CHANNEL"),
        **config,
    }
