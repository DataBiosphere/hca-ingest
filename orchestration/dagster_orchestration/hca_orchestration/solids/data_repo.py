import time

from dagster import configured, Failure, solid, Int
from dagster.core.execution.context.compute import AbstractComputeExecutionContext

from hca_manage.manage import JobId
from hca_orchestration.support.typing import DagsterConfigDict, wrap_as_dagster_type

DagsterJobId = wrap_as_dagster_type(JobId)


@solid(
    required_resource_keys={'data_repo_client'},
    config_schema={
        'max_wait_time_seconds': Int,
        'poll_interval_seconds': Int,
    }
)
def base_wait_for_job_completion(context: AbstractComputeExecutionContext, job_id: DagsterJobId) -> JobId:
    time_waited = 0

    max_wait_time = context.solid_config['max_wait_time_seconds']

    while time_waited < max_wait_time:
        job_info = context.resources.data_repo_client.retrieve_job(job_id)
        if job_info.completed:
            return job_id

        time.sleep(context.solid_config['poll_interval_seconds'])
        time_waited += context.solid_config['poll_interval_seconds']

    raise Failure(f"Exceeded max wait time of {max_wait_time} polling for status of job {job_id}.")


@configured(base_wait_for_job_completion)
def wait_for_job_completion(config: DagsterConfigDict) -> DagsterConfigDict:
    return {
        'max_wait_time_seconds': 60 * 60 * 3,  # 3 hours
        'poll_interval_seconds': 15
    }
