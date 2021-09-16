from dagster import configured, EventMetadataEntry, Failure, Int, solid
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from dagster_utils.contrib.data_repo import jobs
from dagster_utils.typing import DagsterConfigDict

from hca_manage.common import JobId


@solid(
    required_resource_keys={'data_repo_client'},
    config_schema={
        'max_wait_time_seconds': Int,
        'poll_interval_seconds': Int,
    }
)
def base_wait_for_job_completion(context: AbstractComputeExecutionContext, job_id: JobId) -> JobId:
    max_wait_time_seconds = context.solid_config['max_wait_time_seconds']
    poll_interval_seconds = context.solid_config['poll_interval_seconds']
    client = context.resources.data_repo_client

    try:
        return jobs.poll_job(job_id, max_wait_time_seconds, poll_interval_seconds, client)
    except jobs.JobPollException as e:
        raise Failure(
            description=e.message,
            metadata_entries=[
                EventMetadataEntry.text(job_id, "job_id", description="Failing data repo job ID")
            ]
        )


@configured(base_wait_for_job_completion)
def wait_for_job_completion(config: DagsterConfigDict) -> DagsterConfigDict:
    return {
        'max_wait_time_seconds': 60 * 60 * 24,  # 1 day
        'poll_interval_seconds': 15
    }
