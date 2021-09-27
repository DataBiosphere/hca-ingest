from typing import Optional

from dagster import solid, Failure
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from dagster_utils.contrib.data_repo.typing import JobId

from hca_manage.check import CheckManager
from hca_orchestration.contrib.slack import key_value_slack_blocks
from hca_orchestration.models.hca_dataset import TdrDataset


@solid(
    required_resource_keys={'slack', 'target_hca_dataset', 'dagit_config', 'data_repo_client'}
)
def validate_and_notify(
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
        raise Failure("Dataset failed validation")

    kvs = {
        "Staging area": context.run_config["solids"]["pre_process_metadata"]["config"]["input_prefix"],
        "Target Dataset": context.resources.target_hca_dataset.dataset_name,
        "Jade Project": context.resources.target_hca_dataset.project_id,
        "Dagit link": f'<{context.resources.dagit_config.run_url(context.run_id)}|View in Dagit>'
    }
    context.resources.slack.send_message(blocks=key_value_slack_blocks("HCA Completed Import", key_values=kvs))


@solid(
    required_resource_keys={'slack', 'target_hca_dataset', 'dagit_config'}
)
def initial_solid(context: AbstractComputeExecutionContext) -> None:
    kvs = {
        "Staging area": context.run_config["solids"]["pre_process_metadata"]["config"]["input_prefix"],
        "Target Dataset": context.resources.target_hca_dataset.dataset_name,
        "Jade Project": context.resources.target_hca_dataset.project_id,
        "Dagit link": f'<{context.resources.dagit_config.run_url(context.run_id)}|View in Dagit>'
    }
    context.resources.slack.send_message(blocks=key_value_slack_blocks("HCA Start Import", key_values=kvs))
