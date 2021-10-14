from dagster import PipelineDefinition, SensorDefinition
from dagster_utils.resources.bigquery import bigquery_client
from dagster_utils.resources.google_storage import google_storage_client
from dagster_utils.resources.slack import live_slack_client
from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client
from typing import Union

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.pipelines import copy_project
from hca_orchestration.pipelines import cut_snapshot, load_hca
from hca_orchestration.pipelines.validate_ingress import validate_ingress_graph, staging_area_validator
from hca_orchestration.resources import bigquery_service, load_tag
from hca_orchestration.resources.config.scratch import scratch_config
from hca_orchestration.resources.config.target_hca_dataset import build_new_target_hca_dataset
from hca_orchestration.resources.data_repo_service import data_repo_service
from hca_orchestration.resources.hca_project_config import hca_project_copying_config


def validate_ingress_job() -> PipelineDefinition:
    return validate_ingress_graph.to_job(
        name="validate_ingress",
        resource_defs={
            "slack": preconfigure_resource_for_mode(live_slack_client, "prod"),
            "staging_area_validator": staging_area_validator
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


def base_jobs() -> list[Union[PipelineDefinition, SensorDefinition]]:
    defs = [
        cut_snapshot,
        load_hca,
        validate_ingress_job()
    ]

    return defs
