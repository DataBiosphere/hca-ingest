import os
import warnings

import sentry_sdk
from dagster import (
    ExperimentalWarning,
    HookContext,
    PipelineDefinition,
    graph,
    in_process_executor,
    success_hook,
)
from dagster_gcp.gcs import gcs_pickle_io_manager
from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client
from dagster_utils.resources.google_storage import google_storage_client
from dagster_utils.resources.sam import sam_client
from dagster_utils.resources.slack import live_slack_client

# isort: split

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.resources.config.dagit import dagit_config
from hca_orchestration.resources.config.data_repo import (
    hca_manage_config,
    project_snapshot_creation_config,
)
from hca_orchestration.resources.data_repo_service import data_repo_service
from hca_orchestration.solids.create_snapshot import (
    get_snapshot_from_project,
    make_snapshot_public,
)

warnings.filterwarnings("ignore", category=ExperimentalWarning)

SENTRY_DSN = os.getenv(
    "SENTRY_DSN",
    "",
)
if SENTRY_DSN:
    sentry_sdk.init(dsn=SENTRY_DSN, traces_sample_rate=1.0)


def make_snapshot_public_job(hca_env: str, jade_env: str) -> PipelineDefinition:
    return set_snapshot_public.to_job(
        description="Given a source project ID, this pipeline will determine the corresponding "
                    "snapshot_id and use Sam to set the snapshot to public.",
        name=f"make_snapshot_public_job_{jade_env}",
        resource_defs={
            "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, jade_env),
            "data_repo_service": data_repo_service,
            "gcs": google_storage_client,
            "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, hca_env),
            "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, hca_env),
            "sam_client": preconfigure_resource_for_mode(sam_client, hca_env),
            "slack": preconfigure_resource_for_mode(live_slack_client, hca_env),
            "snapshot_config": project_snapshot_creation_config,
            "dagit_config": preconfigure_resource_for_mode(dagit_config, hca_env),
        },
        config={
            "resources": {
                "snapshot_config": {
                    "config": {
                        "managed_access": False,
                        "source_hca_project_id": "",
                        "qualifier": None,
                        "atlas": "hca"
                    }
                }
            },
        },
        executor_def=in_process_executor
    )


@success_hook(
    required_resource_keys={'slack', 'snapshot_config', 'dagit_config', 'hca_manage_config'}
)
def make_public_start_notification(context: HookContext) -> None:
    context.log.info(f"Solid output = {context.solid_output_values}")
    snapshot_id = context.solid_output_values["result"]
    lines = ("HCA Starting Make Snapshot Public",
             f"Snapshot name: {context.resources.snapshot_config.snapshot_name}",
             f"Snapshot ID: {snapshot_id}",
             f"Google Project ID: {context.resources.hca_manage_config.google_project_name}",
             f"Dataset: {context.resources.snapshot_config.dataset_name}",
             "Dagit link: " + f"<{context.resources.dagit_config.run_url(context.run_id)}|View in Dagit>"
             )

    slack_msg_text = "\n".join(lines)
    context.resources.slack.send_message(text=slack_msg_text)


@success_hook(
    required_resource_keys={'slack', 'snapshot_config', 'dagit_config', 'hca_manage_config', 'data_repo_client'}
)
def message_for_snapshot_public(context: HookContext) -> None:
    snapshot_id = context.solid_output_values["result"]
    snapshot_details = context.resources.data_repo_client.retrieve_snapshot(
        id=snapshot_id,
        include=["DATA_PROJECT"]
    )
    data_repo_project = snapshot_details.data_project

    lines = ("HCA Snapshot Public",
             f"Snapshot name: {context.resources.snapshot_config.snapshot_name}",
             f"Dataset Google Project ID: {context.resources.hca_manage_config.google_project_name}",
             f"Source Dataset: {context.resources.snapshot_config.dataset_name}",
             f"Snapshot Google Data Project ID: {data_repo_project}",
             "Dagit link: " + f"<{context.resources.dagit_config.run_url(context.run_id)}|View in Dagit>"
             )
    slack_msg_text = "\n".join(lines)
    context.resources.slack.send_message(text=slack_msg_text)


@graph
def set_snapshot_public() -> None:
    hooked_get_snapshot_from_project = get_snapshot_from_project.with_hooks({make_public_start_notification})
    hooked_make_snapshot_public = make_snapshot_public.with_hooks({message_for_snapshot_public})

    hooked_make_snapshot_public(
        hooked_get_snapshot_from_project())
