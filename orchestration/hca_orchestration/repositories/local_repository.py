"""
Pipelines here are intended to be run in a local context on a developer's box
"""

from dagster import repository, PipelineDefinition, in_process_executor
from dagster_gcp.gcs import gcs_pickle_io_manager
from dagster_utils.resources.beam.local_beam_runner import local_beam_runner
from dagster_utils.resources.bigquery import bigquery_client
from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client
from dagster_utils.resources.google_storage import google_storage_client
from dagster_utils.resources.sam import sam_client
from dagster_utils.resources.slack import console_slack_client, live_slack_client

from hca_orchestration.contrib.dagster import configure_partitions_for_pipeline
from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.pipelines.load_hca import load_hca
from hca_orchestration.pipelines.cut_snapshot import cut_snapshot
from hca_orchestration.pipelines.validate_ingress import run_config_for_validation_ingress_partition, \
    validate_ingress_graph, staging_area_validator
from hca_orchestration.resources import load_tag, bigquery_service
from hca_orchestration.resources.config.dagit import dagit_config
from hca_orchestration.resources.config.scratch import scratch_config
from hca_orchestration.resources.config.target_hca_dataset import target_hca_dataset
from hca_orchestration.resources.data_repo_service import data_repo_service
from hca_orchestration.resources.config.data_repo import snapshot_creation_config, hca_manage_config


def validate_ingress_job() -> PipelineDefinition:
    return validate_ingress_graph.to_job(
        name="validate_ingress",
        resource_defs={
            "slack": console_slack_client,
            "staging_area_validator": staging_area_validator
        }
    )


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


def cut_snapshot_job() -> PipelineDefinition:
    return cut_snapshot.to_job(
        resource_defs={
            "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
            "data_repo_service": data_repo_service,
            "gcs": google_storage_client,
            "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "dev"),
            "sam_client": preconfigure_resource_for_mode(sam_client, "dev"),
            "slack": preconfigure_resource_for_mode(live_slack_client, "local"),
            "snapshot_config": snapshot_creation_config,
            "dagit_config": preconfigure_resource_for_mode(dagit_config, "local"),
        },
        executor_def=in_process_executor
    )


@repository
def all_jobs() -> list[PipelineDefinition]:
    jobs = [
        load_hca_job(),
        cut_snapshot_job(),
        validate_ingress_job()
    ]
    jobs += configure_partitions_for_pipeline("validate_ingress", run_config_for_validation_ingress_partition)
    return jobs
