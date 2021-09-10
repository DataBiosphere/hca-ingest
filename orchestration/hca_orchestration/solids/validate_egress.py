from dagster import solid, String
from dagster.core.execution.context.compute import AbstractComputeExecutionContext

from hca_manage.common import ProblemCount
from hca_manage.check import CheckManager


@solid(
    required_resource_keys={'data_repo_client', 'hca_dataset_operation_config', 'hca_manage_config'}
)
def post_import_validate(context: AbstractComputeExecutionContext) -> ProblemCount:
    """
    Checks if the target dataset has any rows with duplicate IDs or null file references.
    """
    return CheckManager(
        environment=context.resources.hca_manage_config.gcp_env,
        project=context.resources.hca_manage_config.google_project_name,
        dataset=context.resources.hca_dataset_operation_config.dataset_name,
        data_repo_client=context.resources.data_repo_client
    ).check_for_all()


@solid(
    required_resource_keys={'slack', 'hca_dataset_operation_config'},
    config_schema={
        "argo_workflow_id": String
    }
)
def notify_slack_of_egress_validation_results(
    context: AbstractComputeExecutionContext,
    validation_results: ProblemCount,
) -> str:
    gcp_env = context.resources.hca_dataset_operation_config.gcp_env
    dataset_name = context.resources.hca_dataset_operation_config.dataset_name
    argo_workflow_id = context.solid_config["argo_workflow_id"]

    if validation_results.has_problems():
        message_lines = [
            f"Problems identified in post-validation for HCA {gcp_env} dataset {dataset_name}:",
            f"Triggering Argo workflow ID: {argo_workflow_id}",
            "Duplicate lines found: " + str(validation_results.duplicates),
            "Null file references found: " + str(validation_results.null_file_refs),
            "Dangling project references found: " + str(validation_results.dangling_project_refs),
            "Empty links table: " + str(validation_results.empty_links_count),
            "Empty projects table: " + str(validation_results.empty_projects_count)
        ]
    else:
        message_lines = [
            f"HCA {gcp_env} dataset {dataset_name} has passed post-validation.",
            f"Argo Workflow ID: {argo_workflow_id}"]

    message = "\n".join(message_lines)

    context.resources.slack.send_message(message)

    return message
