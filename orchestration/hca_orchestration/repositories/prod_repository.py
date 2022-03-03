"""
Pipelines here are intended to be run in the PROD HCA GCP project
"""

from dagster import PipelineDefinition, in_process_executor, repository
from dagster_gcp.gcs import gcs_pickle_io_manager
from dagster_utils.resources.beam.k8s_beam_runner import (
    k8s_dataflow_beam_runner,
)
from dagster_utils.resources.bigquery import bigquery_client
from dagster_utils.resources.data_repo.jade_data_repo import (
    jade_data_repo_client,
)
from dagster_utils.resources.google_storage import google_storage_client
from dagster_utils.resources.slack import live_slack_client

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.config.dcp_release.dcp_release import (
    run_config_for_dcp_release_partition,
    run_config_for_dcp_release_per_project_partition,
)
from hca_orchestration.config.prod_migration.prod_migration import (
    run_config_cut_project_snapshot_job_real_prod_dcp2,
    run_config_for_real_prod_migration_dcp1,
    run_config_for_real_prod_migration_dcp2,
    run_config_per_project_snapshot_job,
)
from hca_orchestration.contrib.dagster import configure_partitions_for_pipeline
from hca_orchestration.pipelines import copy_project
from hca_orchestration.pipelines.cut_snapshot import (
    cut_project_snapshot_job,
    legacy_cut_snapshot_job,
)
from hca_orchestration.pipelines.load_hca import load_hca
from hca_orchestration.pipelines.validate_ingress import (
    run_config_for_validation_ingress_partition,
    staging_area_validator,
    validate_ingress_graph,
)
from hca_orchestration.repositories.common import (
    build_pipeline_failure_sensor,
    slack_on_pipeline_start,
    slack_on_pipeline_success,
)
from hca_orchestration.resources import bigquery_service, load_tag
from hca_orchestration.resources.config.dagit import dagit_config
from hca_orchestration.resources.config.datasets import (
    find_or_create_project_dataset,
    passthrough_hca_dataset,
)
from hca_orchestration.resources.config.scratch import scratch_config
from hca_orchestration.resources.data_repo_service import data_repo_service
from hca_orchestration.resources.hca_project_config import (
    hca_project_copying_config,
    hca_project_id,
)
from hca_orchestration.resources.utils import run_start_time


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
        name="load_hca",
        resource_defs={
            "beam_runner": preconfigure_resource_for_mode(k8s_dataflow_beam_runner, "prod"),
            "bigquery_client": bigquery_client,
            "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "prod"),
            "gcs": google_storage_client,
            "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "prod"),
            "load_tag": load_tag,
            "scratch_config": scratch_config,
            "target_hca_dataset": passthrough_hca_dataset,
            "bigquery_service": bigquery_service,
            "data_repo_service": data_repo_service,
            "slack": preconfigure_resource_for_mode(live_slack_client, "prod"),
            "dagit_config": preconfigure_resource_for_mode(dagit_config, "prod")
        },
        executor_def=in_process_executor
    )


def per_project_load_hca() -> PipelineDefinition:
    return load_hca.to_job(
        name="per_project_load_hca",
        resource_defs={
            "beam_runner": preconfigure_resource_for_mode(k8s_dataflow_beam_runner, "prod"),
            "bigquery_client": bigquery_client,
            "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "real_prod"),
            "gcs": google_storage_client,
            "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "prod"),
            "load_tag": load_tag,
            "scratch_config": scratch_config,
            "target_hca_dataset": find_or_create_project_dataset,
            "bigquery_service": bigquery_service,
            "data_repo_service": data_repo_service,
            "run_start_time": run_start_time,
            "hca_project_id": hca_project_id,
            "slack": preconfigure_resource_for_mode(live_slack_client, "prod"),
            "dagit_config": preconfigure_resource_for_mode(dagit_config, "prod")
        },
        executor_def=in_process_executor
    )


def dcp1_real_prod_migration() -> PipelineDefinition:
    return copy_project.to_job(
        name=f"dcp1_real_prod_migration",
        description=f"Copies a DCP1 project from prod to real_prod",
        resource_defs={
            "bigquery_client": bigquery_client,
            "bigquery_service": bigquery_service,
            "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "real_prod"),
            "data_repo_service": data_repo_service,
            "gcs": google_storage_client,
            "hca_project_id": hca_project_id,
            "hca_project_copying_config": hca_project_copying_config,
            "load_tag": load_tag,
            "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "prod"),
            "run_start_time": run_start_time,
            "target_hca_dataset": find_or_create_project_dataset,
            "scratch_config": scratch_config,
        },
        executor_def=in_process_executor
    )


def dcp2_real_prod_migration() -> PipelineDefinition:
    return copy_project.to_job(
        name=f"dcp2_real_prod_migration",
        description=f"Copies a DCP2 project from prod to real_prod",
        resource_defs={
            "bigquery_client": bigquery_client,
            "bigquery_service": bigquery_service,
            "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "real_prod"),
            "data_repo_service": data_repo_service,
            "gcs": google_storage_client,
            "hca_project_copying_config": hca_project_copying_config,
            "hca_project_id": hca_project_id,
            "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "prod"),
            "target_hca_dataset": find_or_create_project_dataset,
            "load_tag": load_tag,
            "run_start_time": run_start_time,
            "scratch_config": scratch_config,
        },
        executor_def=in_process_executor
    )


@repository
def all_jobs() -> list[PipelineDefinition]:
    jobs = [
        per_project_load_hca(),
        dcp1_real_prod_migration(),
        dcp2_real_prod_migration(),
        cut_project_snapshot_job("prod", "prod", "monster@firecloud.org"),
        cut_project_snapshot_job("prod", "real_prod", "monster@firecloud.org"),
        legacy_cut_snapshot_job("prod", "monster@firecloud.org"),
        load_hca_job(),
        validate_ingress_job(),
        slack_on_pipeline_start,
        slack_on_pipeline_success,
        build_pipeline_failure_sensor()
    ]
    jobs += configure_partitions_for_pipeline("dcp1_real_prod_migration",
                                              run_config_for_real_prod_migration_dcp1)
    jobs += configure_partitions_for_pipeline("dcp2_real_prod_migration",
                                              run_config_for_real_prod_migration_dcp2)
    jobs += configure_partitions_for_pipeline("cut_project_snapshot_job_real_prod",
                                              run_config_per_project_snapshot_job)
    jobs += configure_partitions_for_pipeline("load_hca", run_config_for_dcp_release_partition)
    jobs += configure_partitions_for_pipeline("per_project_load_hca",
                                              run_config_for_dcp_release_per_project_partition)
    jobs += configure_partitions_for_pipeline("validate_ingress", run_config_for_validation_ingress_partition)

    return jobs
