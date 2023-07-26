"""
Pipelines here are intended to be run in the DEV HCA GCP project
"""

from dagster import PipelineDefinition, in_process_executor, repository
from dagster_gcp.gcs import gcs_pickle_io_manager
from dagster_utils.resources.beam.k8s_beam_runner import k8s_dataflow_beam_runner
from dagster_utils.resources.bigquery import bigquery_client
from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client
from dagster_utils.resources.google_storage import google_storage_client
from dagster_utils.resources.slack import live_slack_client

# isort: split

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.config.dcp_release.dcp_release import (
    dev_run_config_for_dcp_release_per_project_partition,
)
from hca_orchestration.config.dev_refresh.dev_refresh import (
    run_config_for_per_project_dataset_partition,
)
from hca_orchestration.config.prod_migration.prod_migration import (
    run_config_per_project_snapshot_job,
)
from hca_orchestration.contrib.dagster import configure_partitions_for_pipeline
from hca_orchestration.pipelines.cut_snapshot import (
    cut_project_snapshot_job,
    legacy_cut_snapshot_job,
)
from hca_orchestration.pipelines.load_hca import load_hca
from hca_orchestration.pipelines.set_snapshot_public import make_snapshot_public_job
from hca_orchestration.pipelines.validate_ingress import (
    run_config_for_validation_ingress_partition,
    staging_area_validator,
    validate_ingress_graph,
)
from hca_orchestration.repositories.common import copy_project_to_new_dataset_job
from hca_orchestration.resources import bigquery_service, load_tag
from hca_orchestration.resources.config.dagit import dagit_config
from hca_orchestration.resources.config.datasets import (
    find_or_create_project_dataset,
    passthrough_hca_dataset,
)
from hca_orchestration.resources.config.scratch import scratch_config
from hca_orchestration.resources.data_repo_service import data_repo_service
from hca_orchestration.resources.utils import run_start_time


def validate_ingress_job() -> PipelineDefinition:
    return validate_ingress_graph.to_job(
        name="validate_ingress",
        resource_defs={
            "slack": preconfigure_resource_for_mode(live_slack_client, "dev"),
            "staging_area_validator": staging_area_validator,
            "gcs": google_storage_client
        },
        executor_def=in_process_executor
    )


def load_hca_job() -> PipelineDefinition:
    return load_hca.to_job(
        resource_defs={
            "beam_runner": preconfigure_resource_for_mode(k8s_dataflow_beam_runner, "dev"),
            "bigquery_client": bigquery_client,
            "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
            "gcs": google_storage_client,
            "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "dev"),
            "load_tag": load_tag,
            "scratch_config": scratch_config,
            "target_hca_dataset": passthrough_hca_dataset,
            "bigquery_service": bigquery_service,
            "data_repo_service": data_repo_service,
            "slack": preconfigure_resource_for_mode(live_slack_client, "dev"),
            "dagit_config": preconfigure_resource_for_mode(dagit_config, "dev")
        },
        executor_def=in_process_executor
    )


def project_load_hca_job() -> PipelineDefinition:
    return load_hca.to_job(
        resource_defs={
            "beam_runner": preconfigure_resource_for_mode(k8s_dataflow_beam_runner, "dev"),
            "bigquery_client": bigquery_client,
            "bigquery_service": bigquery_service,
            "dagit_config": preconfigure_resource_for_mode(dagit_config, "dev"),
            "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
            "data_repo_service": data_repo_service,
            "gcs": google_storage_client,
            "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "dev"),
            "load_tag": load_tag,
            "run_start_time": run_start_time,
            "scratch_config": scratch_config,
            "slack": preconfigure_resource_for_mode(live_slack_client, "dev"),
            "target_hca_dataset": find_or_create_project_dataset,
        }
    )


@repository
def all_jobs() -> list[PipelineDefinition]:
    jobs = [
        copy_project_to_new_dataset_job("prod", "dev"),
        make_snapshot_public_job("dev", "dev"),
        cut_project_snapshot_job("dev", "dev", "monster-dev@dev.test.firecloud.org"),
        legacy_cut_snapshot_job("dev", "monster-dev@dev.test.firecloud.org"),
        load_hca_job(),
        validate_ingress_job()
    ]
    jobs += configure_partitions_for_pipeline("copy_project_to_new_dataset",
                                              run_config_for_per_project_dataset_partition)
    # jobs += configure_partitions_for_pipeline("make_snapshot_public_job_dev",
    # run_config_for_cut_snapshot_partition) # old partition method?
    jobs += configure_partitions_for_pipeline("make_snapshot_public_job_real_prod",
                                              run_config_per_project_snapshot_job)
    # jobs += configure_partitions_for_pipeline("cut_snapshot", run_config_for_cut_snapshot_partition) # old?
    jobs += configure_partitions_for_pipeline("cut_project_snapshot_job_dev",
                                              run_config_per_project_snapshot_job)
    jobs += configure_partitions_for_pipeline("load_hca", dev_run_config_for_dcp_release_per_project_partition)
    jobs += configure_partitions_for_pipeline("validate_ingress", run_config_for_validation_ingress_partition)

    return jobs
