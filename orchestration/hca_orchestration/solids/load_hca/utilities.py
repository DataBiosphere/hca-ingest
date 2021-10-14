from dagster import solid, Failure
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from dagster_utils.contrib.data_repo.typing import JobId
from typing import Optional

from hca_manage.check import CheckManager
from hca_orchestration.contrib.dagster import short_run_id
from hca_orchestration.models.hca_dataset import TdrDataset
from hca_orchestration.solids.validate_egress import construct_validation_message


@solid(
    required_resource_keys={'slack', 'target_hca_dataset', 'dagit_config', 'data_repo_client'}
)
def validate_and_send_finish_notification(
        context: AbstractComputeExecutionContext,
        results1: list[Optional[JobId]],
        results2: list[Optional[JobId]]
) -> None:
    target_hca_dataset: TdrDataset = context.resources.target_hca_dataset
    check_result = CheckManager(
        environment="dev",
        project=target_hca_dataset.project_id,
        dataset=target_hca_dataset.dataset_name,
        data_repo_client=context.resources.data_repo_client,
        snapshot=False
    ).check_for_all()

    if check_result.has_problems():
        message = construct_validation_message(
            check_result,
            target_hca_dataset.dataset_name,
            short_run_id(context.run_id)
        )
        context.resources.slack.send_message(message)
        raise Failure(f"Dataset {target_hca_dataset.dataset_id} failed validation")
    else:
        lines = ("HCA Completed Import",
                 f'Staging area: {context.run_config["solids"]["pre_process_metadata"]["config"]["input_prefix"]}',
                 f"Target Dataset: {context.resources.target_hca_dataset.dataset_name}",
                 f"Jade Project: {context.resources.target_hca_dataset.project_id}",
                 "Dagit link: " + f'<{context.resources.dagit_config.run_url(context.run_id)}|View in Dagit>'
                 )
        slack_msg_text = "\n".join(lines)
        context.resources.slack.send_message(text=slack_msg_text)


@solid(
    required_resource_keys={'slack', 'target_hca_dataset', 'dagit_config'}
)
def send_start_notification(context: AbstractComputeExecutionContext) -> None:
    lines = (
        f'Staging area: {context.run_config["solids"]["pre_process_metadata"]["config"]["input_prefix"]}',
        f"Target Dataset: {context.resources.target_hca_dataset.dataset_name}",
        f"Jade Project: {context.resources.target_hca_dataset.project_id}",
        "Dagit link: " + f'<{context.resources.dagit_config.run_url(context.run_id)}|View in Dagit>'
    )
    slack_msg_text = "\n".join(lines)
    context.resources.slack.send_message(text=slack_msg_text)
