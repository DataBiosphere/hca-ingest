import os

from dagster import ModeDefinition, pipeline
from dagster.core.execution.context.system import HookContext
from dagster_slack import slack_on_failure, slack_on_success

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
    return f"Cutting snapshot '{context.solid_config['snapshot_name']}' for "\
           f"dataset '{context.solid_config['dataset_name']}'."


def message_for_job_failed(context: HookContext) -> str:
    return f"""
        FAILED to cut a snapshot for dataset {context.solid_config['dataset_name']}!
        The snapshot name and data repo job ID can be found on the Dagster dashboard.
    """


def message_for_snapshot_done(context: HookContext) -> str:
    snapshot_name = context.solid_config['snapshot_name']
    return f"""
        Snapshot '{snapshot_name}' for dataset '{context.solid_config['dataset_name']}' complete.
    """


@pipeline(
    mode_defs=[prod_mode, local_mode, test_mode]
)
def cut_snapshot() -> None:
    slack_channel = os.environ.get("SLACK_NOTIFICATIONS_CHANNEL")
    dagit_url = os.environ.get("DAGIT_BASE_URL")
    hooked_submit_snapshot_job = submit_snapshot_job.with_hooks({
        slack_on_success(slack_channel, message_for_snapshot_start, dagit_url)
    })
    hooked_make_snapshot_public = make_snapshot_public.with_hooks({
        slack_on_success(slack_channel, message_for_snapshot_done, dagit_url)
    })
    hooked_wait_for_job_completion = wait_for_job_completion.with_hooks({
        slack_on_failure(slack_channel, message_for_job_failed, dagit_url)
    })

    hooked_make_snapshot_public(
        get_completed_snapshot_info(
            hooked_wait_for_job_completion(
                hooked_submit_snapshot_job())))
