from dagster import configured, EventMetadataEntry, Failure, Int, solid
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from hca_manage.manage import JobId
from hca_orchestration.contrib.retry import retry, is_truthy, RetryException
from hca_orchestration.support.typing import DagsterConfigDict

from data_repo_client import RepositoryApi


def _retrieve_job(data_repo_client: RepositoryApi, job_id: JobId) -> bool:
    job_info = data_repo_client.retrieve_job(job_id)
    if job_info.completed:
        if job_info.job_status == "failed":
            raise Failure(
                description="Job did not complete successfully.",
                metadata_entries=[
                    EventMetadataEntry.text(job_id, "job_id", description="Failing data repo job ID")
                ]
            )
        return True
    return False


@solid(
    required_resource_keys={'data_repo_client'},
    config_schema={
        'max_wait_time_seconds': Int,
        'poll_interval_seconds': Int,
    }
)
def base_wait_for_job_completion(context: AbstractComputeExecutionContext, job_id: JobId) -> JobId:
    max_wait_time = context.solid_config["max_wait_time_seconds"]
    poll_interval = context.solid_config["poll_interval_seconds"]

    try:
        retry(
            _retrieve_job,
            max_wait_time,
            poll_interval,
            is_truthy,
            context.resources.data_repo_client,
            job_id
        )
    except RetryException:
        raise Failure(f"Exceeded max wait time of {max_wait_time} polling for status of job {job_id}.")
    return job_id


@configured(base_wait_for_job_completion)
def wait_for_job_completion(config: DagsterConfigDict) -> DagsterConfigDict:
    return {
        'max_wait_time_seconds': 60 * 60 * 24,  # 1 day
        'poll_interval_seconds': 15
    }
