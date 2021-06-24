from typing import Optional

from dagster import solid, Int, Failure, Nothing, configured, String, DagsterLogManager
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from dagster_utils.typing import DagsterConfigDict
from data_repo_client import JobModel, ApiException, RepositoryApi

from hca_manage.common import JobId
from hca_orchestration.contrib.retry import is_truthy, retry


@solid(
    required_resource_keys={"data_repo_client"},
    config_schema={
        'max_wait_time_seconds': Int,
        'poll_interval_seconds': Int,
    }
)
def base_check_data_ingest_job_result(context: AbstractComputeExecutionContext, job_id: JobId) -> JobId:
    job_results = _base_check_jade_job_result(
        context.solid_config['max_wait_time_seconds'],
        context.solid_config['poll_interval_seconds'],
        job_id,
        context.resources.data_repo_client,
        context.log
    )
    if job_results['failedFiles'] > 0:
        raise Failure(f"Bulk file load (job_id = {job_id} had failedFiles = {job_results['failedFiles']})")

    return job_id


@configured(base_check_data_ingest_job_result)
def check_data_ingest_job_result(config: DagsterConfigDict) -> DagsterConfigDict:
    """
    Polls the bulk file ingest results
    Any files failed will fail the pipeline
    """
    return {
        'max_wait_time_seconds': 28800,  # 8 hours
        'poll_interval_seconds': 5
    }


@solid(
    required_resource_keys={"data_repo_client"},
    config_schema={
        'max_wait_time_seconds': Int,
        'poll_interval_seconds': Int,
    }
)
def check_table_ingest_result(context: AbstractComputeExecutionContext, job_id: JobId) -> JobId:
    job_results = _base_check_jade_job_result(
        context.solid_config['max_wait_time_seconds'],
        context.solid_config['poll_interval_seconds'],
        job_id,
        context.resources.data_repo_client,
        context.log
    )
    if job_results['bad_row_count'] == '0':
        raise Failure(f"Bulk file load (job_id = {job_id} had failedFiles = {job_results['failedFiles']})")

    return job_id


@configured(check_table_ingest_result)
def check_table_ingest_job_result(config: DagsterConfigDict) -> DagsterConfigDict:
    """
    Polls the bulk file ingest results
    Any files failed will fail the pipeline
    """
    return {
        'max_wait_time_seconds': 600,  # 10 minutes
        'poll_interval_seconds': 5,
    }


def _base_check_jade_job_result(
        max_wait_time_seconds: int,
        poll_interval_seconds: int,
        job_id: JobId,
        data_repo_client: RepositoryApi,
        logger: DagsterLogManager
) -> Nothing:
    # we need to poll on the endpoint as a workaround for a race condition in TDR (DR-1791)
    def __fetch_job_results(jid: JobId) -> Optional[JobModel]:
        try:
            logger.info(f"Fetching job results for job_id = {jid}")
            return data_repo_client.retrieve_job_result(jid)
        except ApiException as ae:
            if 500 <= ae.status <= 599:
                logger.info(f"Data repo returned error when fetching results for job_id = {jid}, scheduling retry")
                return None
            raise

    job_results = retry(
        __fetch_job_results,
        max_wait_time_seconds,
        poll_interval_seconds,
        is_truthy,
        job_id
    )
    if not job_results:
        raise Failure(f"No job results after polling bulk ingest, job_id = {job_id}")

    return job_results
