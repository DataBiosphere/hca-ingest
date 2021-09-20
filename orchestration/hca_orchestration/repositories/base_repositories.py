import os
from typing import Union

from dagster import PipelineDefinition, repository, SensorDefinition
from dagster_utils.resources.bigquery import bigquery_client
from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client
from dagster_utils.resources.google_storage import google_storage_client

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.config.dev_refresh.dev_refresh import dev_refresh_per_project_dataset_partition_set, \
    dev_refresh_cut_snapshot_partition_set
from hca_orchestration.config.dcp_release.dcp_release import load_dcp_release_manifests
from hca_orchestration.pipelines import copy_project
from hca_orchestration.pipelines import cut_snapshot, load_hca, validate_egress
from hca_orchestration.resources import bigquery_service, load_tag
from hca_orchestration.resources.config.scratch import scratch_config
from hca_orchestration.resources.config.target_hca_dataset import build_new_target_hca_dataset
from hca_orchestration.resources.data_repo_service import data_repo_service
from hca_orchestration.resources.hca_project_config import hca_project_copying_config
from hca_orchestration.sensors import build_post_import_sensor


def copy_project_to_new_dataset_job() -> PipelineDefinition:
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


def base_jobs() -> list[Union[PipelineDefinition, SensorDefinition]]:
    defs = [
        cut_snapshot,
        load_hca,
        validate_egress,
        build_post_import_sensor(os.environ.get("ENV", "test")),
        copy_project_to_new_dataset_job(),
        dev_refresh_per_project_dataset_partition_set(),
        dev_refresh_cut_snapshot_partition_set(),
    ]
    return defs + load_dcp_release_manifests()
