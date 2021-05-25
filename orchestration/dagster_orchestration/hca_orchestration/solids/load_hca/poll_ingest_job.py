from typing import Optional

from dagster import solid, Int, Failure, Nothing, configured
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from dagster_utils.typing import DagsterConfigDict
from data_repo_client import JobModel, ApiException

from hca_manage.common import JobId
from hca_orchestration.contrib.retry import is_truthy, retry


@solid(
    required_resource_keys={"data_repo_client"},
    config_schema={
        'max_wait_time_seconds': Int,
        'poll_interval_seconds': Int,
    }
)
def base_check_data_ingest_job_result(context: AbstractComputeExecutionContext, job_id: JobId) -> Nothing:
    # we need to poll on the endpoint as a workaround for a race condition in TDR (DR-1791)
    def __fetch_job_results(jid: JobId) -> Optional[JobModel]:
        try:
            context.log.info(f"Fetching job results for job_id = {jid}")
            return context.resources.data_repo_client.retrieve_job_result(jid)
        except ApiException as ae:
            if 500 <= ae.status <= 599:
                context.log.info(f"Data repo returned error when fetching results for job_id = {jid}, scheduling retry")
                return None
            raise

    job_results = retry(
        __fetch_job_results,
        context.solid_config['max_wait_time_seconds'],
        context.solid_config['poll_interval_seconds'],
        is_truthy,
        job_id
    )
    if not job_results:
        raise Failure(f"No job results after polling bulk ingest, job_id = {job_id}")

    if job_results['failedFiles'] > 0:
        raise Failure(f"Bulk file load (job_id = {job_id} had failedFiles = {job_results['failedFiles']})")


@configured(base_check_data_ingest_job_result)
def check_data_ingest_job_result(config: DagsterConfigDict) -> DagsterConfigDict:
    """
    Polls the bulk file ingest results
    Any files failed will fail the pipeline
    """
    return {
        'max_wait_time_seconds': 600,  # 10 minutes
        'poll_interval_seconds': 5
    }
