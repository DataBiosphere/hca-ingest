from dagster import ModeDefinition, pipeline, success_hook, failure_hook
from dagster.core.execution.context.system import HookContext

from dagster_gcp.gcs import gcs_pickle_io_manager

from dagster_utils.resources.google_storage import google_storage_client
from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client, noop_data_repo_client
from dagster_utils.resources.sam import sam_client, noop_sam_client
from dagster_utils.resources.slack import console_slack_client, live_slack_client

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.solids.create_snapshot import get_completed_snapshot_info, make_snapshot_public, submit_snapshot_job
from hca_orchestration.solids.data_repo import wait_for_job_completion
from hca_orchestration.resources.config.dagit import dagit_config
from hca_orchestration.resources.config.data_repo import hca_manage_config, snapshot_creation_config


prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "prod"),
        "gcs": google_storage_client,
        "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "prod"),
        "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "prod"),
        "sam_client": preconfigure_resource_for_mode(sam_client, "prod"),
        "slack": preconfigure_resource_for_mode(live_slack_client, "prod"),
        "snapshot_config": snapshot_creation_config,
        "dagit_config": preconfigure_resource_for_mode(dagit_config, "prod"),
    }
)

dev_mode = ModeDefinition(
    name="dev",
    resource_defs={
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
        "gcs": google_storage_client,
        "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "dev"),
        "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "dev"),
        # we don't want to actually hit sam and make a snapshot public
        # unless we're running in prod
        "sam_client": noop_sam_client,
        "slack": preconfigure_resource_for_mode(live_slack_client, "dev"),
        "snapshot_config": snapshot_creation_config,
        "dagit_config": preconfigure_resource_for_mode(dagit_config, "dev"),
    }
)

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
        "gcs": google_storage_client,
        "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "dev"),
        "sam_client": noop_sam_client,
        "slack": preconfigure_resource_for_mode(live_slack_client, "local"),
        "snapshot_config": snapshot_creation_config,
        "dagit_config": preconfigure_resource_for_mode(dagit_config, "local"),
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
        "dagit_config": preconfigure_resource_for_mode(dagit_config, "test"),
    }
)


@success_hook(
    required_resource_keys={'slack', 'snapshot_config', 'dagit_config'}
)
def snapshot_start_notification(context: HookContext) -> None:
    message = (
        f"Cutting snapshot '{context.resources.snapshot_config.snapshot_name}' "
        f"for dataset '{context.resources.snapshot_config.dataset_name}'.\n"
        f"<{context.resources.dagit_config.run_url(context.run_id)}|View in Dagit>"
    )

    context.resources.slack.send_message(message)


@failure_hook(
    required_resource_keys={'slack', 'snapshot_config', 'dagit_config'}
)
def snapshot_job_failed_notification(context: HookContext) -> None:
    message = (
        f"FAILED to cut snapshot '{context.resources.snapshot_config.snapshot_name}' "
        f"for dataset '{context.resources.snapshot_config.dataset_name}!\n"
        f"<{context.resources.dagit_config.run_url(context.run_id)}|View in Dagit>"
    )

    context.resources.slack.send_message(message)


@success_hook(
    required_resource_keys={'slack', 'snapshot_config', 'dagit_config'}
)
def message_for_snapshot_done(context: HookContext) -> None:
    message = (
        f"COMPLETED snapshot '{context.resources.snapshot_config.snapshot_name}' "
        f"for dataset '{context.resources.snapshot_config.dataset_name}'.\n"
        f"<{context.resources.dagit_config.run_url(context.run_id)}|View in Dagit>"
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
