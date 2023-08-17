import os
from typing import Any
from unittest.mock import patch, MagicMock, Mock

import pytest
from dagster import file_relative_path, ResourceDefinition, Failure, JobDefinition
from dagster.core.execution.execution_results import InProcessGraphResult
from dagster.utils import load_yaml_from_globs
from dagster.utils.merger import deep_merge_dicts
from dagster_utils.resources.sam import Sam
from dagster_utils.resources.slack import console_slack_client
from data_repo_client import RepositoryApi, SnapshotModel

from google.cloud.storage import Client

import hca_orchestration.resources.data_repo_service
from hca_orchestration.contrib.data_repo.data_repo_service import DataRepoService
from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.models.hca_dataset import TdrDataset
from hca_orchestration.pipelines import cut_snapshot, load_hca, set_snapshot_public, validate_ingress_graph
from hca_orchestration.resources import load_tag
from hca_orchestration.resources.config.dagit import dagit_config
from hca_orchestration.resources.config.data_repo import hca_manage_config, snapshot_creation_config
from hca_orchestration.resources.config.scratch import scratch_config
from hca_orchestration.resources.config.datasets import passthrough_hca_dataset
from hca_manage.validation import HcaValidator


def config_path(relative_path: str) -> str:
    path: str = file_relative_path(
        __file__, os.path.join("../environments/", relative_path)
    )
    return path


def beam_runner_path() -> str:
    path: str = file_relative_path(__file__, '../../../')
    return path


def run_pipeline(
        job: JobDefinition,
        config_name: str,
        extra_config: dict[str, Any] = {},
) -> InProcessGraphResult:
    config_dict = load_yaml_from_globs(
        config_path(config_name)
    )
    config_dict = deep_merge_dicts(config_dict, extra_config)

    return job.execute_in_process(
        run_config=config_dict,
    )


@patch("hca_manage.bq_managers.NullFileRefManager.get_rows")
@patch("hca_manage.bq_managers.NullFileRefManager.get_file_table_names")
@patch("hca_manage.bq_managers.DuplicatesManager.get_rows")
@patch("hca_manage.bq_managers.DuplicatesManager.get_all_table_names")
@patch("hca_manage.bq_managers.DanglingFileRefManager.get_rows")
@patch("hca_manage.bq_managers.CountsManager.get_rows")
def test_load_hca_noop_resources(*mocks):
    data_repo_service = Mock(hca_orchestration.resources.data_repo_service.DataRepoService)
    data_repo_service.get_dataset = Mock(return_value=TdrDataset("fake", "fake", "fake", "fake", "fake"))
    job = load_hca.to_job(
        resource_defs={
            "beam_runner": ResourceDefinition.mock_resource(),
            "gcs": ResourceDefinition.mock_resource(),
            "data_repo_client": ResourceDefinition.mock_resource(),
            "bigquery_client": ResourceDefinition.mock_resource(),
            "load_tag": load_tag,
            "scratch_config": scratch_config,
            "target_hca_dataset": passthrough_hca_dataset,
            "bigquery_service": ResourceDefinition.mock_resource(),
            "data_repo_service": ResourceDefinition.hardcoded_resource(data_repo_service),
            "slack": console_slack_client,
            "dagit_config": preconfigure_resource_for_mode(dagit_config, "test")
        }
    )

    result = run_pipeline(job, config_name="test_load_hca_noop_resources.yaml")

    assert result.success
    scratch_dataset_name = result.result_for_node("create_scratch_dataset").output_value("result")
    assert scratch_dataset_name.startswith(
        "fake_bq_project.testing_dataset_prefix_fake"), "staging dataset should start with load tag prefix"


def test_validate_ingress_success():
    validator = Mock(spec=HcaValidator)
    validator.validate_staging_area = Mock(return_value=0)
    job = validate_ingress_graph.to_job(resource_defs={
        "slack": console_slack_client,
        "staging_area_validator": ResourceDefinition.hardcoded_resource(validator),
        "gcs": ResourceDefinition.hardcoded_resource(Mock(spec=Client))
    }, config=load_yaml_from_globs(
        config_path("test_validate_ingress.yaml")
    ))

    mock_validator = MagicMock()
    mock_validator.validate_staging_area = MagicMock(return_value=0)
    result = job.execute_in_process()

    assert result.success


def test_validate_ingress_failure():
    job = validate_ingress_graph.to_job(resource_defs={
        "slack": console_slack_client,
        "staging_area_validator": ResourceDefinition.hardcoded_resource(Mock(spec=HcaValidator)),
        "gcs": ResourceDefinition.hardcoded_resource(Mock(spec=Client))
    }, config=load_yaml_from_globs(
        config_path("test_validate_ingress.yaml")
    ))

    mock_validator = MagicMock()
    mock_validator.validate_staging_area = MagicMock(return_value=1)

    with pytest.raises(Failure):
        job.execute_in_process()


def test_cut_snapshot(*mocks):
    data_repo = MagicMock(spec=RepositoryApi)
    data_repo.retrieve_job_result = MagicMock(
        return_value={
            "id": "fake_object_id",
            "name": "fake_object_name",
            "failedFiles": 0})
    data_repo.retrieve_snapshot = MagicMock(
        return_value=SnapshotModel(data_project="fake_data_project")
    )
    job = cut_snapshot.to_job(
        resource_defs={
            "data_repo_client": ResourceDefinition.hardcoded_resource(data_repo),
            "data_repo_service": ResourceDefinition.hardcoded_resource(Mock(spec=DataRepoService)),
            "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "test"),
            "sam_client": ResourceDefinition.hardcoded_resource(Mock(spec=Sam)),
            "slack": console_slack_client,
            "snapshot_config": snapshot_creation_config,
            "dagit_config": preconfigure_resource_for_mode(dagit_config, "test"),
            "run_start_time": ResourceDefinition.hardcoded_resource(123456)
        }
    )
    result = run_pipeline(job, config_name="test_create_snapshot.yaml")

    assert result.success


def test_set_snapshot_public(*mocks):
    data_repo = MagicMock(spec=RepositoryApi)
    data_repo.enumerate_snapshots = MagicMock(
        return_value={
            "total": 1,
            "filteredTotal": 1,
            "items": [
                {
                    "id": "fake_object_id",
                    "name": "fake_object_name"
                }
            ]
        }
    )
    job = set_snapshot_public.to_job(
        resource_defs={
            "data_repo_client": ResourceDefinition.hardcoded_resource(data_repo),
            "data_repo_service": ResourceDefinition.hardcoded_resource(Mock(spec=DataRepoService)),
            "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "test"),
            "sam_client": ResourceDefinition.hardcoded_resource(Mock(spec=Sam)),
            "slack": console_slack_client,
            "snapshot_config": snapshot_creation_config,
            "dagit_config": preconfigure_resource_for_mode(dagit_config, "test"),
            "run_start_time": ResourceDefinition.hardcoded_resource(123456)
        }
    )
    result = run_pipeline(job, config_name="test_make_snapshot_public.yaml")
    assert result.success
