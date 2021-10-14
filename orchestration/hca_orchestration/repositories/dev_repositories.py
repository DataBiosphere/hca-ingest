from dagster import PipelineDefinition, repository
from dagster_gcp.gcs import gcs_pickle_io_manager
from dagster_utils.resources.beam.k8s_beam_runner import k8s_dataflow_beam_runner
from dagster_utils.resources.bigquery import bigquery_client
from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client
from dagster_utils.resources.google_storage import google_storage_client
from dagster_utils.resources.slack import live_slack_client

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.config.dev_refresh.dev_refresh import dev_refresh_cut_snapshot_partition_set, \
    copy_project_to_new_dataset_partitions
from hca_orchestration.pipelines import copy_project
from hca_orchestration.pipelines.load_hca import load_hca
from hca_orchestration.pipelines.validate_ingress import validate_ingress_graph, staging_area_validator
from hca_orchestration.resources import bigquery_service
from hca_orchestration.resources import load_tag
from hca_orchestration.resources.config.dagit import dagit_config
from hca_orchestration.resources.config.scratch import scratch_config
from hca_orchestration.resources.config.target_hca_dataset import target_hca_dataset, build_new_target_hca_dataset
from hca_orchestration.resources.data_repo_service import data_repo_service
from hca_orchestration.resources.hca_project_config import hca_project_copying_config


def validate_ingress_job() -> PipelineDefinition:
    return validate_ingress_graph.to_job(
        name="validate_ingress",
        resource_defs={
            "slack": preconfigure_resource_for_mode(live_slack_client, "dev"),
            "staging_area_validator": staging_area_validator
        }
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
            "target_hca_dataset": target_hca_dataset,
            "bigquery_service": bigquery_service,
            "data_repo_service": data_repo_service,
            "slack": preconfigure_resource_for_mode(live_slack_client, "dev"),
            "dagit_config": preconfigure_resource_for_mode(dagit_config, "dev")
        }
    )


def copy_project_to_new_dataset_job() -> PipelineDefinition:
    return copy_project.to_job(
        name="copy_project_to_new_dataset",
        resource_defs={
            "bigquery_client": bigquery_client,
            "data_repo_client": jade_data_repo_client,
            "gcs": google_storage_client,
            "scratch_config": scratch_config,
            "bigquery_service": bigquery_service,
            "hca_project_copying_config": hca_project_copying_config,
            "target_hca_dataset": build_new_target_hca_dataset,
            "load_tag": load_tag,
            "data_repo_service": data_repo_service,
        })


@repository
def all_jobs() -> list[PipelineDefinition]:
    jobs = [copy_project_to_new_dataset_job(), load_hca_job()]
    jobs += copy_project_to_new_dataset_partitions()
    jobs += dev_refresh_cut_snapshot_partition_set()
    return jobs
