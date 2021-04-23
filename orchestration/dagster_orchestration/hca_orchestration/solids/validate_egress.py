from dagster import configured, solid, String, StringSource
from dagster.core.execution.context.compute import AbstractComputeExecutionContext

from hca_manage.manage import HcaManage, ProblemCount
from hca_orchestration.support.typing import DagsterConfigDict


@solid(
    required_resource_keys={'data_repo_client'},
    config_schema={
        "gcp_env": StringSource,
        "dataset_name": String,
        "google_project_name": StringSource,
    }
)
def base_post_import_validate(context: AbstractComputeExecutionContext) -> ProblemCount:
    """
    Checks if the target dataset has any rows with duplicate IDs or null file references.
    """
    return HcaManage(
        environment=context.solid_config["gcp_env"],
        project=context.solid_config["google_project_name"],
        dataset=context.solid_config["dataset_name"],
        data_repo_client=context.resources.data_repo_client
    ).check_for_all()


# sets up default config settings to minimize pipeline boilerplate
@configured(base_post_import_validate, {"dataset_name": String})
def post_import_validate(config: DagsterConfigDict) -> DagsterConfigDict:
    return {
        'gcp_env': {'env': 'HCA_GCP_ENV'},
        'google_project_name': {'env': 'DATA_REPO_GOOGLE_PROJECT'},
        **config,
    }


@solid(
    required_resource_keys={"slack"},
    config_schema={
        "gcp_env": StringSource,
        "dataset_name": String,
        "channel": StringSource,
        "argo_workflow_id": String
    }
)
def base_notify_slack_of_egress_validation_results(
    context: AbstractComputeExecutionContext,
    validation_results: ProblemCount,
) -> str:
    gcp_env = context.solid_config["gcp_env"]
    dataset_name = context.solid_config["dataset_name"]
    argo_workflow_id = context.solid_config["argo_workflow_id"]

    if validation_results.has_problems():
        message_lines = [
            f"Problems identified in post-validation for HCA {gcp_env} dataset {dataset_name}:",
            f"Triggering Argo workflow ID: {argo_workflow_id}",
            "Duplicate lines found: " + str(validation_results.duplicates),
            "Null file references found: " + str(validation_results.null_file_refs),
            "Dangling project references found: " + str(validation_results.dangling_project_refs)
        ]
    else:
        message_lines = [
            f"HCA {gcp_env} dataset {dataset_name} has passed post-validation.",
            f"Argo Workflow ID: {argo_workflow_id}"]

    message = "\n".join(message_lines)

    context.resources.slack.chat_postMessage(
        channel=context.solid_config["channel"],
        text=message)

    return message


@configured(base_notify_slack_of_egress_validation_results, {"dataset_name": String, "argo_workflow_id": String})
def notify_slack_of_egress_validation_results(config: DagsterConfigDict) -> DagsterConfigDict:
    return {
        'gcp_env': {'env': 'HCA_GCP_ENV'},
        'channel': {'env': 'SLACK_NOTIFICATIONS_CHANNEL'},
        **config,
    }
