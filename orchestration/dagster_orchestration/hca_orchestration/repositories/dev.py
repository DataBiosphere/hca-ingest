from typing import Union

from dagster import PipelineDefinition, repository, SensorDefinition
from dagster_utils.resources.bigquery import bigquery_client
from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client
from dagster_utils.resources.google_storage import google_storage_client

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.pipelines import copy_project
from hca_orchestration.resources import bigquery_service, load_tag
from hca_orchestration.resources.config.scratch import scratch_config
from hca_orchestration.resources.config.target_hca_dataset import target_hca_dataset, build_new_target_hca_dataset
from hca_orchestration.resources.hca_project_config import hca_project_copying_config
from hca_orchestration.resources.data_repo_service import data_repo_service


def copy_project_job():
    return copy_project.to_job(resource_defs={
        "bigquery_client": bigquery_client,
        "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
        "gcs": google_storage_client,
        "scratch_config": scratch_config,
        "bigquery_service": bigquery_service,
        "hca_project_copying_config": hca_project_copying_config,
        "target_hca_dataset": target_hca_dataset,
        "load_tag": load_tag
    })


def copy_project_to_new_dataset_job():
    return copy_project.to_job(
        name="copy_project_to_new_dataset",
        resource_defs={
            "bigquery_client": bigquery_client,
            "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
            "gcs": google_storage_client,
            "scratch_config": scratch_config,
            "bigquery_service": bigquery_service,
            "hca_project_copying_config": hca_project_copying_config,
            "target_hca_dataset": build_new_target_hca_dataset,
            "load_tag": load_tag,
            "data_repo_service": data_repo_service,
        })


@repository
def dev_repo() -> list[Union[PipelineDefinition, SensorDefinition]]:
    return [copy_project_job(), copy_project_to_new_dataset_job()]
