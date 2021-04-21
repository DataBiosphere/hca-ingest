from dagster import composite_solid, Noneable, solid, String
from dagster.core.execution.context.compute import AbstractComputeExecutionContext

from data_repo_client import SnapshotModel

from hca_manage.manage import JobId
from hca_orchestration.solids.data_repo import wait_for_job_completion
from hca_orchestration.support.hca_manage import hca_manage_from_solid_context
from hca_orchestration.support.schemas import HCA_MANAGE_SCHEMA


@solid(
    required_resource_keys={'data_repo_client'},
    config_schema={
        **HCA_MANAGE_SCHEMA,
        'qualifier': Noneable(String),
    }
)
def submit_snapshot_job(context: AbstractComputeExecutionContext) -> JobId:
    return hca_manage_from_solid_context(context).submit_snapshot_request(context.solid_config['qualifier'])


# every 'get job results' solid will look exactly like this, but with a distinct return type depending
# on the kind of result the job will have.
@solid(
    required_resource_keys={'data_repo_client'}
)
def get_completed_snapshot_info(context: AbstractComputeExecutionContext, job_id: JobId) -> SnapshotModel:
    return context.resources.data_repo_client.retrieve_job_result(job_id)


@composite_solid(
    config_schema={
        **HCA_MANAGE_SCHEMA,
        'qualifier': Noneable(String),
    },
    config_fn=lambda composite_config: {'submit_snapshot_job': {'config': composite_config}}
)
def create_snapshot() -> SnapshotModel:
    return get_completed_snapshot_info(wait_for_job_completion(submit_snapshot_job()))


@solid(
    required_resource_keys={'sam_client'}
)
def make_snapshot_public(context: AbstractComputeExecutionContext, snapshot_info: SnapshotModel) -> None:
    context.resources.sam_client.make_snapshot_public(snapshot_info.id)
