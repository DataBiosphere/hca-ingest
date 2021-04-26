import os
from urllib.parse import urljoin

from dagster import ModeDefinition, pipeline, success_hook, failure_hook
from dagster.core.execution.context.system import HookContext

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.solids.create_snapshot import get_completed_snapshot_info, make_snapshot_public, submit_snapshot_job
from hca_orchestration.solids.data_repo import wait_for_job_completion
from hca_orchestration.resources.data_repo import jade_data_repo_client, noop_data_repo_client
from hca_orchestration.resources.config.data_repo import hca_manage_config, snapshot_creation_config
from hca_orchestration.resources.slack import console_slack_client, live_slack_client
from hca_orchestration.resources.sam import sam_client, noop_sam_client


prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "prod"),
        "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "prod"),
        "sam_client": preconfigure_resource_for_mode(sam_client, "prod"),
        "slack": preconfigure_resource_for_mode(live_slack_client, "prod"),
        "snapshot_config": snapshot_creation_config,
    }
)

dev_mode = ModeDefinition(
    name="dev",
    resource_defs={
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
        "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "dev"),
        # we don't want to actually hit sam and make a snapshot public
        # unless we're running in prod
        "sam_client": noop_sam_client,
        "slack": preconfigure_resource_for_mode(live_slack_client, "dev"),
        "snapshot_config": snapshot_creation_config,
    }
)

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
        "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "dev"),
        "sam_client": noop_sam_client,
        "slack": preconfigure_resource_for_mode(live_slack_client, "local"),
        "snapshot_config": snapshot_creation_config,
    }
)

test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "data_repo_client": noop_data_repo_client,
        "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "test"),
        "sam_client": noop_sam_client,
        "slack": console_slack_client,
        "snapshot_config": snapshot_creation_config,
    }
)


def dagit_run_url(context: HookContext) -> str:
    dagit_url = os.environ["DAGIT_BASE_URL"]
    return urljoin(dagit_url, f"instance/runs/{context.run_id}")


@success_hook(
    required_resource_keys={'slack', 'snapshot_config'}
)
def snapshot_start_notification(context: HookContext) -> None:
    run_url = dagit_run_url(context)
    message = (
        f"Cutting snapshot '{context.resources.snapshot_config.snapshot_name}' "
        f"for dataset '{context.resources.snapshot_config.dataset_name}'.\n"
        f"<{run_url}|View in Dagit>"
    )

    context.resources.slack.send_message(message)


@failure_hook(
    required_resource_keys={'slack', 'snapshot_config'}
)
def snapshot_job_failed_notification(context: HookContext) -> None:
    run_url = dagit_run_url(context)
    message = (
        f"FAILED to cut snapshot '{context.resources.snapshot_config.snapshot_name}' "
        f"for dataset '{context.resources.snapshot_config.dataset_name}!\n"
        f"<{run_url}|View in Dagit>"
    )

    context.resources.slack.send_message(message)


@success_hook(
    required_resource_keys={'slack', 'snapshot_config'}
)
def message_for_snapshot_done(context: HookContext) -> None:
    run_url = dagit_run_url(context)
    message = (
        f"COMPLETED snapshot '{context.resources.snapshot_config.snapshot_name}' "
        f"for dataset '{context.resources.snapshot_config.dataset_name}'.\n"
        f"<{run_url}|View in Dagit>"
    )

    context.resources.slack.send_message(message)


@pipeline(
    mode_defs=[prod_mode, dev_mode, local_mode, test_mode]
)
def cut_snapshot() -> None:
    hooked_submit_snapshot_job = submit_snapshot_job.with_hooks({snapshot_start_notification})
    hooked_wait_for_job_completion = wait_for_job_completion.with_hooks({snapshot_job_failed_notification})
    hooked_make_snapshot_public = make_snapshot_public.with_hooks({message_for_snapshot_done})

    hooked_make_snapshot_public(
        get_completed_snapshot_info(
            hooked_wait_for_job_completion(
                hooked_submit_snapshot_job())))
