from dagster import ModeDefinition, pipeline, success_hook, failure_hook, ResourceDefinition, PresetDefinition
from dagster import HookContext

from dagster_gcp.gcs import gcs_pickle_io_manager

from dagster_utils.resources.google_storage import google_storage_client
from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client, noop_data_repo_client
from dagster_utils.resources.sam import sam_client, noop_sam_client
from dagster_utils.resources.slack import console_slack_client, live_slack_client
from data_repo_client import SnapshotRequestAccessIncludeModel

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.contrib.slack import key_value_slack_blocks
from hca_orchestration.resources.data_repo_service import data_repo_service
from hca_orchestration.solids.create_snapshot import get_completed_snapshot_info, make_snapshot_public, \
    submit_snapshot_job, add_steward
from hca_orchestration.solids.data_repo import wait_for_job_completion
from hca_orchestration.resources.config.dagit import dagit_config
from hca_orchestration.resources.config.data_repo import hca_manage_config, snapshot_creation_config, \
    dev_refresh_snapshot_creation_config

real_prod_mode = ModeDefinition(
    name="real_prod",
    resource_defs={
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "real_prod"),
        "gcs": google_storage_client,
        "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "prod"),
        "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "prod"),
        "sam_client": preconfigure_resource_for_mode(sam_client, "prod"),
        "slack": preconfigure_resource_for_mode(live_slack_client, "prod"),
        "snapshot_config": snapshot_creation_config,
        "dagit_config": preconfigure_resource_for_mode(dagit_config, "prod"),
    }
)

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
        "sam_client": preconfigure_resource_for_mode(sam_client, "dev"),
        "slack": preconfigure_resource_for_mode(live_slack_client, "dev"),
        "snapshot_config": snapshot_creation_config,
        "dagit_config": preconfigure_resource_for_mode(dagit_config, "dev"),
    }
)

dev_refresh_mode = ModeDefinition(
    name="dev_refresh",
    resource_defs={
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
        "gcs": google_storage_client,
        "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "dev"),
        "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "dev"),
        "sam_client": preconfigure_resource_for_mode(sam_client, "dev"),
        "slack": console_slack_client,  # preconfigure_resource_for_mode(live_slack_client, "dev"),
        "snapshot_config": dev_refresh_snapshot_creation_config,
        "dagit_config": preconfigure_resource_for_mode(dagit_config, "dev"),
        "data_repo_service": data_repo_service
    }
)

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
        "gcs": google_storage_client,
        "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "dev"),
        "sam_client": preconfigure_resource_for_mode(sam_client, "dev"),
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
    required_resource_keys={'slack', 'snapshot_config', 'dagit_config', 'hca_manage_config'}
)
def snapshot_start_notification(context: HookContext) -> None:
    context.log.info(f"Solid output = {context.solid_output_values}")
    job_id = context.solid_output_values["result"]
    kvs = {
        "Snapshot name": context.resources.snapshot_config.snapshot_name,
        "Google Project ID": context.resources.hca_manage_config.google_project_name,
        "Dataset": context.resources.snapshot_config.dataset_name,
        "TDR Job ID": job_id,
        "Dagit link": f'<{context.resources.dagit_config.run_url(context.run_id)}|View in Dagit>'

    }

    context.resources.slack.send_message(blocks=key_value_slack_blocks("HCA Starting Snapshot", key_values=kvs))


@failure_hook(
    required_resource_keys={'slack', 'snapshot_config', 'dagit_config', 'hca_manage_config'}
)
def snapshot_job_failed_notification(context: HookContext) -> None:
    if "result" in context.solid_output_values:
        job_id = context.solid_output_values["result"]
    else:
        job_id = "N/A"

    kvs = {
        "Snapshot name": context.resources.snapshot_config.snapshot_name,
        "Dataset Google Project ID": context.resources.hca_manage_config.google_project_name,
        "Source Dataset": context.resources.snapshot_config.dataset_name,
        "TDR Job ID": job_id,
        "Dagit link": f'<{context.resources.dagit_config.run_url(context.run_id)}|View in Dagit>'
    }

    context.resources.slack.send_message(blocks=key_value_slack_blocks("HCA Snapshot Failed", key_values=kvs))


@success_hook(
    required_resource_keys={'slack', 'snapshot_config', 'dagit_config', 'hca_manage_config', 'data_repo_client'}
)
def message_for_snapshot_done(context: HookContext) -> None:
    snapshot_id = context.solid_output_values["result"]
    snapshot_details: SnapshotRequestAccessIncludeModel = context.resources.data_repo_client.retrieve_snapshot(
        id=snapshot_id,
        include=["DATA_PROJECT"]
    )
    data_repo_project = snapshot_details.data_project

    context.resources.slack.send_message(blocks=key_value_slack_blocks("HCA Snapshot Complete", {
        "Snapshot name": context.resources.snapshot_config.snapshot_name,
        "Dataset Google Project ID": context.resources.hca_manage_config.google_project_name,
        "Source Dataset": context.resources.snapshot_config.dataset_name,
        "Snapshot Google Data Project ID": data_repo_project,
        "Dagit link": f'<{context.resources.dagit_config.run_url(context.run_id)}|View in Dagit>'
    }))


@pipeline(
    mode_defs=[prod_mode, dev_mode, local_mode, test_mode, dev_refresh_mode],
    preset_defs=[
        PresetDefinition("dev_preset", mode="dev", run_config={
            "solids": {
                "add_steward": {
                    "config": {
                        "snapshot_steward": "monster-dev@dev.test.firecloud.org"
                    }
                }
            }
        }),
        PresetDefinition("prod_preset", mode="prod", run_config={
            "solids": {
                "add_steward": {
                    "config": {
                        "snapshot_steward": "monster@firecloud.org"
                    }
                }
            }
        })
    ]
)
def cut_snapshot() -> None:
    hooked_submit_snapshot_job = submit_snapshot_job.with_hooks({snapshot_start_notification})
    hooked_wait_for_job_completion = wait_for_job_completion.with_hooks({snapshot_job_failed_notification})
    hooked_make_snapshot_public = make_snapshot_public.with_hooks({message_for_snapshot_done})

    hooked_make_snapshot_public(
        add_steward(
            get_completed_snapshot_info(
                hooked_wait_for_job_completion(
                    hooked_submit_snapshot_job())))
    )
