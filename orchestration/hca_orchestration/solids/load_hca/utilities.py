from typing import Optional

from dagster import solid
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from dagster_utils.contrib.data_repo.typing import JobId

from hca_orchestration.contrib.slack import key_value_slack_blocks


@solid(
    required_resource_keys={'slack', 'target_hca_dataset', 'dagit_config'}
)
def terminal_solid(
        context: AbstractComputeExecutionContext,
        results1: list[Optional[JobId]],
        results2: list[Optional[JobId]]
) -> None:
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
