"""
Pipelines here are intended to be run in the PROD HCA GCP project
"""

from dagster import PipelineDefinition, repository, ConfigMapping
from dagster_gcp.gcs import gcs_pickle_io_manager
from dagster_utils.resources.beam.k8s_beam_runner import k8s_dataflow_beam_runner
from dagster_utils.resources.bigquery import bigquery_client
from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client
from dagster_utils.resources.google_storage import google_storage_client
from dagster_utils.resources.sam import sam_client
from dagster_utils.resources.slack import live_slack_client

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.config.dcp_release.dcp_release import load_dcp_release_manifests
from hca_orchestration.pipelines.load_hca import load_hca
from hca_orchestration.pipelines.cut_snapshot import cut_snapshot
from hca_orchestration.pipelines.validate_ingress import validate_ingress_graph, staging_area_validator
from hca_orchestration.resources import load_tag
from hca_orchestration.resources.config.scratch import scratch_config
from hca_orchestration.resources.config.target_hca_dataset import target_hca_dataset
from hca_orchestration.resources import bigquery_service
from hca_orchestration.resources.data_repo_service import data_repo_service
from hca_orchestration.resources.config.dagit import dagit_config
from hca_orchestration.resources.config.data_repo import hca_manage_config, snapshot_creation_config


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
        }
    )


def cut_snapshot_job():
    return cut_snapshot.to_job(
        resource_defs={
            "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "prod"),
            "gcs": google_storage_client,
            "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "prod"),
            "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "prod"),
            "sam_client": preconfigure_resource_for_mode(sam_client, "prod"),
            "slack": preconfigure_resource_for_mode(live_slack_client, "prod"),
            "snapshot_config": snapshot_creation_config,
            "dagit_config": preconfigure_resource_for_mode(dagit_config, "prod"),
        }
    )


def cut_snapshot_real_prod_job():
    return cut_snapshot.to_job(
        name="cut_snapshot_real_prod",  # disambiguate
        resource_defs={
            "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "real_prod"),
            "gcs": google_storage_client,
            "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "prod"),
            "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "prod"),
            "sam_client": preconfigure_resource_for_mode(sam_client, "prod"),
            "slack": preconfigure_resource_for_mode(live_slack_client, "prod"),
            "snapshot_config": snapshot_creation_config,
            "dagit_config": preconfigure_resource_for_mode(dagit_config, "prod"),
        }
    )


@repository
def all_jobs() -> list[PipelineDefinition]:
    jobs = [
        load_hca_job(),
        cut_snapshot_job(),
        validate_ingress_job(),
        cut_snapshot_real_prod_job()
    ]
    jobs += load_dcp_release_manifests()

    return jobs
