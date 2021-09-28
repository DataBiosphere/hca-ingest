from dagster import solid
from dagster.core.execution.context.compute import AbstractComputeExecutionContext

from hca_manage.check import CheckManager
from hca_manage.common import ProblemCount
from hca_orchestration.contrib.dagster import short_run_id


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
        data_repo_client=context.resources.data_repo_client,
        snapshot=False
    ).check_for_all()


@solid(
    required_resource_keys={'slack', 'hca_dataset_operation_config'}
)
def notify_slack_of_egress_validation_results(
    context: AbstractComputeExecutionContext,
    validation_results: ProblemCount
) -> str:
    dataset_name = context.resources.hca_dataset_operation_config.dataset_name
    message = construct_validation_message(validation_results, dataset_name, short_run_id(context.run_id))
    context.resources.slack.send_message(message)

    return message


def construct_validation_message(validation_results: ProblemCount, dataset_name: str, run_id: str) -> str:
    if validation_results.has_problems():
        message_lines = [
            f"Problems identified in post-validation for HCA dataset {dataset_name}:",
            "Run ID: " + str(run_id),
            "Duplicate lines found: " + str(validation_results.duplicates),
            "Null file references found: " + str(validation_results.null_file_refs),
            "Dangling project references found: " + str(validation_results.dangling_project_refs),
            "Empty links table: " + str(validation_results.empty_links_count),
            "Empty projects table: " + str(validation_results.empty_projects_count)
        ]
    else:
        message_lines = [
            f"HCA dataset {dataset_name} has passed post-validation."
        ]

    message = "\n".join(message_lines)
    return message
