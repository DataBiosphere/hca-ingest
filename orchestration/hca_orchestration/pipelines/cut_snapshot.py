import warnings

from dagster import HookContext
from dagster import success_hook, failure_hook, graph, \
    ExperimentalWarning
from data_repo_client import SnapshotRequestAccessIncludeModel

from hca_orchestration.solids.create_snapshot import get_completed_snapshot_info, make_snapshot_public, \
    submit_snapshot_job, add_steward
from hca_orchestration.solids.data_repo import wait_for_job_completion

warnings.filterwarnings("ignore", category=ExperimentalWarning)


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
    snapshot_details: SnapshotRequestAccessIncludeModel = context.resources.data_repo_client.retrieve_snapshot(
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
