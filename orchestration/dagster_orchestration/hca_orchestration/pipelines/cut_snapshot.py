import os
from urllib.parse import urljoin

from dagster import ModeDefinition, pipeline, success_hook, failure_hook
from dagster.core.execution.context.system import HookContext

from hca_orchestration.solids.create_snapshot import get_completed_snapshot_info, make_snapshot_public, submit_snapshot_job
from hca_orchestration.solids.data_repo import wait_for_job_completion
from hca_orchestration.resources.data_repo import jade_data_repo_client, noop_data_repo_client, snapshot_creation_config
from hca_orchestration.resources.slack import console_slack_client, live_slack_client
from hca_orchestration.resources.sam import prod_sam_client, noop_sam_client


prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "data_repo_client": jade_data_repo_client,
        "sam_client": prod_sam_client,
        "slack": live_slack_client,
        "snapshot_config": snapshot_creation_config,
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
        "snapshot_config": snapshot_creation_config,
    }
)

test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "data_repo_client": noop_data_repo_client,
        "sam_client": noop_sam_client,
        "slack": console_slack_client,
        "snapshot_config": snapshot_creation_config,
    }
)


def slack_hook_info(context: HookContext) -> dict[str, str]:
    slack_channel = os.environ["SLACK_NOTIFICATIONS_CHANNEL"]
    dagit_url = os.environ["DAGIT_BASE_URL"]
    run_url = urljoin(dagit_url, f"instance/runs/{context.run_id}")

    return {
        'slack_channel': slack_channel,
        'run_url': run_url,
    }


@success_hook(
    required_resource_keys={'slack', 'snapshot_config'}
)
def snapshot_start_notification(context: HookContext) -> None:
    hook_info = slack_hook_info(context)
    message = (
        f"Cutting snapshot '{context.resources.snapshot_config.snapshot_name}' "
        f"for dataset '{context.resources.snapshot_config.dataset_name}'.\n"
        f"<{hook_info['run_url']}|View in Dagit>"
    )

    context.resources.slack.chat_postMessage(channel=hook_info['slack_channel'], text=message)


@failure_hook(
    required_resource_keys={'slack', 'snapshot_config'}
)
def snapshot_job_failed_notification(context: HookContext) -> None:
    hook_info = slack_hook_info(context)
    message = (
        f"FAILED to cut snapshot '{context.resources.snapshot_config.snapshot_name}' "
        f"for dataset '{context.resources.snapshot_config.dataset_name}!\n"
        f"<{hook_info['run_url']}|View in Dagit>"
    )

    context.resources.slack.chat_postMessage(channel=hook_info['slack_channel'], text=message)


@success_hook(
    required_resource_keys={'slack', 'snapshot_config'}
)
def message_for_snapshot_done(context: HookContext) -> None:
    hook_info = slack_hook_info(context)
    message = (
        f"COMPLETED snapshot '{context.resources.snapshot_config.snapshot_name}' "
        f"for dataset '{context.resources.snapshot_config.dataset_name}'.\n"
        f"<{hook_info['run_url']}|View in Dagit>"
    )

    context.resources.slack.chat_postMessage(channel=hook_info['slack_channel'], text=message)


@pipeline(
    mode_defs=[prod_mode, local_mode, test_mode]
)
def cut_snapshot() -> None:
    hooked_submit_snapshot_job = submit_snapshot_job.with_hooks({snapshot_start_notification})
    hooked_wait_for_job_completion = wait_for_job_completion.with_hooks({snapshot_job_failed_notification})
    hooked_make_snapshot_public = make_snapshot_public.with_hooks({message_for_snapshot_done})

    hooked_make_snapshot_public(
        get_completed_snapshot_info(
            hooked_wait_for_job_completion(
                hooked_submit_snapshot_job())))
