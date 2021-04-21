import os

from dagster import ModeDefinition, pipeline
from dagster.core.execution.context.system import HookContext
from dagster_slack import slack_on_success

from hca_orchestration.solids.create_snapshot import get_completed_snapshot_info, make_snapshot_public, submit_snapshot_job
from hca_orchestration.solids.data_repo import wait_for_job_completion
from hca_orchestration.resources.data_repo import jade_data_repo_client, noop_data_repo_client
from hca_orchestration.resources.slack import console_slack_client, live_slack_client
from hca_orchestration.resources.sam import prod_sam_client, noop_sam_client


prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "data_repo_client": jade_data_repo_client,
        "sam_client": prod_sam_client,
        "slack": live_slack_client,
    }
)

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "data_repo_client": jade_data_repo_client,
        # we don't want to actually hit sam and make a snapshot public
        # unless we're running in prod
        "sam_client": noop_sam_client,
        "slack": live_slack_client,
    }
)

test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "data_repo_client": noop_data_repo_client,
        "sam_client": noop_sam_client,
        "slack": console_slack_client,
    }
)


def message_for_snapshot_start(context: HookContext) -> str:
    return f"Cutting a snapshot for dataset {context.solid_config['dataset_name']}."


def message_for_snapshot_done(context: HookContext) -> str:
    return f"""
        Snapshot for dataset {context.solid_config['dataset_name']} complete.
        Snapshot ID is "{context.solid.input_dict['snapshot_info'].id}".
    """


@pipeline(
    mode_defs=[prod_mode, local_mode, test_mode]
)
def cut_snapshot() -> None:
    snapshot_job_id = submit_snapshot_job().with_hooks(hook_defs={
        slack_on_success(os.environ.get("SLACK_NOTIFICATIONS_CHANNEL"), message_for_snapshot_start)
    })
    make_snapshot_public(get_completed_snapshot_info(wait_for_job_completion(snapshot_job_id))).with_hooks(hook_defs={
        slack_on_success(os.environ.get("SLACK_NOTIFICATIONS_CHANNEL"), message_for_snapshot_done)
    })
