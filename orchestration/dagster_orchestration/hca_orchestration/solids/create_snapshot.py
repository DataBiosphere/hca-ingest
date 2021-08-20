from typing import Iterator

from dagster import AssetMaterialization, EventMetadataEntry, Output, OutputDefinition, solid
from dagster.core.execution.context.compute import AbstractComputeExecutionContext

from hca_manage.common import JobId, data_repo_profile_ids
from hca_orchestration.contrib.data_repo.snapshot import SnapshotManager


@solid(
    required_resource_keys={'data_repo_client', 'snapshot_config', 'hca_manage_config'}
)
def submit_snapshot_job(context: AbstractComputeExecutionContext) -> JobId:
    return SnapshotManager(
        environment=context.resources.hca_manage_config.gcp_env,
        dataset=context.resources.snapshot_config.dataset_name,
        data_repo_client=context.resources.data_repo_client,
        data_repo_profile_id=data_repo_profile_ids[context.resources.hca_manage_config.gcp_env],
    ).submit_snapshot_request_with_name(
        context.resources.snapshot_config.snapshot_name,
        context.resources.snapshot_config.managed_access
    )


@solid(
    required_resource_keys={'data_repo_client', 'snapshot_config'},
    output_defs=[
        OutputDefinition(name='result', dagster_type=str)
    ],
)
def get_completed_snapshot_info(context: AbstractComputeExecutionContext, job_id: JobId) -> Iterator[Output]:
    # retrieve_job_result returns a raw dict (since it can return many kinds of data), so we need to make
    # a second call to the snapshot endpoint to get the actual SnapshotModel from it
    snapshot_info_dict = context.resources.data_repo_client.retrieve_job_result(job_id)
    yield AssetMaterialization(
        asset_key=snapshot_info_dict['id'],
        description="Dataset snapshot created in the data repo",
        metadata_entries=[
            EventMetadataEntry.text(
                context.resources.snapshot_config.dataset_name,
                "dataset_name",
                description="Dataset name in the data repo"),
            EventMetadataEntry.text(
                snapshot_info_dict['name'],
                "snapshot_name",
                description="Snapshot name in the data repo"),
            EventMetadataEntry.text(snapshot_info_dict['id'], "snapshot_id",
                                    description="Snapshot ID in the data repo"),
            EventMetadataEntry.text(job_id, "job_id", description="Successful data repo job ID"),
        ]
    )
    yield Output(snapshot_info_dict['id'])


@solid(
    required_resource_keys={'sam_client'},
)
def make_snapshot_public(context: AbstractComputeExecutionContext, snapshot_id: str) -> None:
    context.resources.sam_client.make_snapshot_public(snapshot_id)
