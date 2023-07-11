import warnings

from dagster import (
    graph,
    HookContext,
    success_hook,
)

from hca_orchestration.solids.create_snapshot import (
    get_snapshot_from_project,
    make_snapshot_public,
)

warnings.filterwarnings("ignore", category=ExperimentalWarning)

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
