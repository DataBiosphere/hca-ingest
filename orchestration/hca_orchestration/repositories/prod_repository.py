"""
Pipelines here are intended to be run in the PROD HCA GCP project
"""

from dagster import PipelineDefinition, repository, in_process_executor
from dagster_gcp.gcs import gcs_pickle_io_manager
from dagster_utils.resources.beam.k8s_beam_runner import k8s_dataflow_beam_runner
from dagster_utils.resources.bigquery import bigquery_client
from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client
from dagster_utils.resources.google_storage import google_storage_client
from dagster_utils.resources.slack import live_slack_client

from hca_orchestration.config.dev_refresh.dev_refresh import run_config_for_per_project_dataset_partition, \
    run_config_for_cut_snapshot_partition
from hca_orchestration.contrib.dagster import configure_partitions_for_pipeline
from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.config.dcp_release.dcp_release import run_config_for_dcp_release_partition
from hca_orchestration.pipelines.cut_snapshot import cut_project_snapshot_job, legacy_cut_snapshot_job
from hca_orchestration.pipelines.load_hca import load_hca
from hca_orchestration.pipelines.validate_ingress import validate_ingress_graph, staging_area_validator, \
    run_config_for_validation_ingress_partition
from hca_orchestration.resources import bigquery_service
from hca_orchestration.resources import load_tag
from hca_orchestration.resources.config.dagit import dagit_config
from hca_orchestration.resources.config.scratch import scratch_config
from hca_orchestration.resources.config.target_hca_dataset import target_hca_dataset
from hca_orchestration.resources.data_repo_service import data_repo_service
from hca_orchestration.repositories.common import copy_project_to_new_dataset_job


def validate_ingress_job() -> PipelineDefinition:
    return validate_ingress_graph.to_job(
        name="validate_ingress",
        resource_defs={
            "slack": preconfigure_resource_for_mode(live_slack_client, "prod"),
            "staging_area_validator": staging_area_validator,
            "gcs": google_storage_client
        },
        executor_def=in_process_executor
    )


def load_hca_job() -> PipelineDefinition:
    return load_hca.to_job(
        resource_defs={
            "beam_runner": preconfigure_resource_for_mode(k8s_dataflow_beam_runner, "prod"),
            "bigquery_client": bigquery_client,
            "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "prod"),
            "gcs": google_storage_client,
            "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "prod"),
            "load_tag": load_tag,
            "scratch_config": scratch_config,
            "target_hca_dataset": target_hca_dataset,
            "bigquery_service": bigquery_service,
            "data_repo_service": data_repo_service,
            "slack": preconfigure_resource_for_mode(live_slack_client, "prod"),
            "dagit_config": preconfigure_resource_for_mode(dagit_config, "prod")
        },
        executor_def=in_process_executor
    )


@repository
def all_jobs() -> list[PipelineDefinition]:
    jobs = [
        copy_project_to_new_dataset_job(),
        cut_project_snapshot_job("prod", "prod", "monster@firecloud.org"),
        cut_project_snapshot_job("prod", "real_prod", "monster@firecloud.org"),
        legacy_cut_snapshot_job("prod", "monster@firecloud.org"),
        load_hca_job(),
        validate_ingress_job()
    ]
    jobs += configure_partitions_for_pipeline("copy_project_to_new_dataset",
                                              run_config_for_per_project_dataset_partition)
    jobs += configure_partitions_for_pipeline("cut_snapshot", run_config_for_cut_snapshot_partition)
    jobs += configure_partitions_for_pipeline("load_hca", run_config_for_dcp_release_partition)
    jobs += configure_partitions_for_pipeline("validate_ingress", run_config_for_validation_ingress_partition)

    return jobs
