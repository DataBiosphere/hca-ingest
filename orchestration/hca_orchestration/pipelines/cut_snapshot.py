import warnings

from dagster import (
    ExperimentalWarning,
    HookContext,
    PipelineDefinition,
    failure_hook,
    graph,
    in_process_executor,
    success_hook,
)
from dagster_gcp.gcs import gcs_pickle_io_manager
from dagster_utils.resources.data_repo.jade_data_repo import (
    jade_data_repo_client,
)
from dagster_utils.resources.google_storage import google_storage_client
from dagster_utils.resources.sam import sam_client
from dagster_utils.resources.slack import live_slack_client

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.resources.config.dagit import dagit_config
from hca_orchestration.resources.config.data_repo import (
    hca_manage_config,
    project_snapshot_creation_config,
    snapshot_creation_config,
)
from hca_orchestration.resources.data_repo_service import data_repo_service
from hca_orchestration.resources.utils import run_start_time
from hca_orchestration.solids.create_snapshot import (
    add_steward,
    get_completed_snapshot_info,
    make_snapshot_public,
    submit_snapshot_job,
)
from hca_orchestration.solids.data_repo import wait_for_job_completion

warnings.filterwarnings("ignore", category=ExperimentalWarning)


def cut_project_snapshot_job(hca_env: str, jade_env: str, steward: str) -> PipelineDefinition:
    return cut_snapshot.to_job(
        description="Given a source project ID, this pipeline will determine the corresponding "
                    "TDR dataset and cut a snapshot with the correct datetime suffix.",
        name=f"cut_project_snapshot_job_{jade_env}",
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
                    }
                }
            }, "solids": {
                "add_steward": {
                    "config": {
                        "snapshot_steward": steward,
                    },
                }
            }
        },
        executor_def=in_process_executor
    )


def legacy_cut_snapshot_job(hca_env: str, steward: str) -> PipelineDefinition:
    return cut_snapshot.to_job(
        name="legacy_cut_snapshot_job",
        description="Given a source dataset name and target snapshot name, "
                    "this job will cut a new snapshot in TDR. This job should only be used for legacy "
                    "monolithic datasets. All new datasets should be per-project and run through the "
                    "cut_project_snapshot job",
        resource_defs={
            "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, hca_env),
            "data_repo_service": data_repo_service,
            "gcs": google_storage_client,
            "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, hca_env),
            "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, hca_env),
            "sam_client": preconfigure_resource_for_mode(sam_client, hca_env),
            "slack": preconfigure_resource_for_mode(live_slack_client, hca_env),
            "snapshot_config": snapshot_creation_config,
            "dagit_config": preconfigure_resource_for_mode(dagit_config, hca_env),
            "run_start_time": run_start_time
        },
        config={
            "resources": {
                "snapshot_config": {
                    "config": {
                        "managed_access": False,
                        "dataset_name": "",
                        "qualifier": None,
                    }
                }
            }, "solids": {
                "add_steward": {
                    "config": {
                        "snapshot_steward": steward,
                    },
                }
            }
        },
        executor_def=in_process_executor
    )


@success_hook(
    required_resource_keys={'slack', 'snapshot_config', 'dagit_config', 'hca_manage_config'}
)
def snapshot_start_notification(context: HookContext) -> None:
    context.log.info(f"Solid output = {context.solid_output_values}")
    job_id = context.solid_output_values["result"]
    lines = ("HCA Starting Snapshot",
             f"Snapshot name: {context.resources.snapshot_config.snapshot_name}",
             f"Google Project ID: {context.resources.hca_manage_config.google_project_name}",
             f"Dataset: {context.resources.snapshot_config.dataset_name}",
             f"TDR Job ID: {job_id}",
             "Dagit link: " + f"<{context.resources.dagit_config.run_url(context.run_id)}|View in Dagit>"
             )

    slack_msg_text = "\n".join(lines)
    context.resources.slack.send_message(text=slack_msg_text)


@failure_hook(
    required_resource_keys={'slack', 'snapshot_config', 'dagit_config', 'hca_manage_config'}
)
def snapshot_job_failed_notification(context: HookContext) -> None:
    if "result" in context.solid_output_values:
        job_id = context.solid_output_values["result"]
    else:
        job_id = "N/A"

    lines = ("HCA Snapshot Failed",
             f"Snapshot name: {context.resources.snapshot_config.snapshot_name}",
             f"Dataset Google Project ID: {context.resources.hca_manage_config.google_project_name}",
             f"Source Dataset: {context.resources.snapshot_config.dataset_name}",
             f"TDR Job ID: {job_id}",
             "Dagit link: " + f"<{context.resources.dagit_config.run_url(context.run_id)}|View in Dagit>"
             )
    slack_msg_text = "\n".join(lines)
    context.resources.slack.send_message(text=slack_msg_text)


@success_hook(
    required_resource_keys={'slack', 'snapshot_config', 'dagit_config', 'hca_manage_config', 'data_repo_client'}
)
def message_for_snapshot_done(context: HookContext) -> None:
    snapshot_id = context.solid_output_values["result"]
    snapshot_details = context.resources.data_repo_client.retrieve_snapshot(
        id=snapshot_id,
        include=["DATA_PROJECT"]
    )
    data_repo_project = snapshot_details.data_project

    lines = ("HCA Snapshot Complete",
             f"Snapshot name: {context.resources.snapshot_config.snapshot_name}",
             f"Dataset Google Project ID: {context.resources.hca_manage_config.google_project_name}",
             f"Source Dataset: {context.resources.snapshot_config.dataset_name}",
             f"Snapshot Google Data Project ID: {data_repo_project}",
             "Dagit link: " + f"<{context.resources.dagit_config.run_url(context.run_id)}|View in Dagit>"
             )
    slack_msg_text = "\n".join(lines)
    context.resources.slack.send_message(text=slack_msg_text)


@graph
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
