from dagster import repository, PipelineDefinition, in_process_executor
from dagster_gcp.gcs import gcs_pickle_io_manager
from dagster_utils.resources.beam.local_beam_runner import local_beam_runner
from dagster_utils.resources.bigquery import bigquery_client
from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client
from dagster_utils.resources.google_storage import google_storage_client
from dagster_utils.resources.slack import console_slack_client

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.pipelines.load_hca import load_hca
from hca_orchestration.resources import load_tag, bigquery_service
from hca_orchestration.resources.config.dagit import dagit_config
from hca_orchestration.resources.config.scratch import scratch_config
from hca_orchestration.resources.config.target_hca_dataset import target_hca_dataset
from hca_orchestration.resources.data_repo_service import data_repo_service


def load_hca_job() -> PipelineDefinition:
    return load_hca.to_job(
        resource_defs={
            "beam_runner": local_beam_runner,
            "bigquery_client": bigquery_client,
            "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
            "gcs": google_storage_client,
            "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "dev"),
            "load_tag": load_tag,
            "scratch_config": scratch_config,
            "target_hca_dataset": target_hca_dataset,
            "bigquery_service": bigquery_service,
            "data_repo_service": data_repo_service,
            "slack": console_slack_client,
            "dagit_config": preconfigure_resource_for_mode(dagit_config, "local")
        },
        executor_def=in_process_executor
    )


@repository
def all_jobs():
    return [load_hca_job()]
