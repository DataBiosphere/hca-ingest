from dagster import AssetMaterialization, EventMetadataEntry, solid, String
from dagster.core.execution.context.compute import AbstractComputeExecutionContext

from data_repo_client import SnapshotModel

from hca_manage.manage import JobId, HcaManage
from hca_orchestration.support.schemas import HCA_MANAGE_SCHEMA


@solid(
    required_resource_keys={'data_repo_client'},
    config_schema={
        **HCA_MANAGE_SCHEMA,
        'snapshot_name': String,
    }
)
def submit_snapshot_job(context: AbstractComputeExecutionContext) -> JobId:
    return HcaManage(
        environment=context.solid_config["gcp_env"],
        project=context.solid_config["google_project_name"],
        dataset=context.solid_config["dataset_name"],
        data_repo_client=context.resources.data_repo_client
    ).submit_snapshot_request_with_name(context.solid_config['snapshot_name'])


# every 'get job results' solid will look roughly like this, but with a distinct return type and
# a distinct materialized asset, depending on the kind of result the job will have.
@solid(
    required_resource_keys={'data_repo_client'}
)
def get_completed_snapshot_info(context: AbstractComputeExecutionContext, job_id: JobId) -> SnapshotModel:
    snapshot_info = context.resources.data_repo_client.retrieve_job_result(job_id)
    yield AssetMaterialization(
        asset_key=snapshot_info.id,
        description="Dataset snapshot created in the data repo",
        metadata_entries=[
            EventMetadataEntry.text(
                context.solid_config['dataset_name'],
                "dataset_name",
                description="Dataset name in the data repo"),
            EventMetadataEntry.text(snapshot_info.name, "snapshot_name", description="Snapshot name in the data repo"),
            EventMetadataEntry.text(snapshot_info.id, "snapshot_id", description="Snapshot ID in the data repo"),
            EventMetadataEntry.text(job_id, "job_id", description="Successful data repo job ID"),
        ]
    )
    return snapshot_info


@solid(
    required_resource_keys={'sam_client'},
)
def make_snapshot_public(context: AbstractComputeExecutionContext, snapshot_info: SnapshotModel) -> None:
    context.resources.sam_client.make_snapshot_public(snapshot_info.id)
