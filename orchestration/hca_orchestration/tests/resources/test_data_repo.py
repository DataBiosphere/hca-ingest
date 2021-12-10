from unittest.mock import MagicMock, Mock
from datetime import datetime
from re import search

import pytest
from dagster import build_init_resource_context, ResourceDefinition

from hca_orchestration.contrib.data_repo.data_repo_service import DataRepoService
from hca_orchestration.resources.config.data_repo import project_snapshot_creation_config, snapshot_creation_config
from hca_orchestration.models.hca_dataset import TdrDataset
from hca_orchestration.support.dates import dataset_snapshot_formatted_date
from hca_manage.snapshot import LEGACY_SNAPSHOT_NAME_REGEX


def test_project_snapshot_creation_config(monkeypatch):
    monkeypatch.setenv("ENV", "dev")
    data_repo_service = Mock(spec=DataRepoService)
    find_dataset_result = TdrDataset(
        dataset_name="hca_dev_abc123__20210505_dcp1",
        dataset_id="999888",
        project_id="bq_123_999",
        billing_profile_id="fake_billing_profile_id",
        bq_location="us-central1"
    )
    data_repo_service.find_dataset = Mock(return_value=find_dataset_result)
    init_context = build_init_resource_context(
        resources={"data_repo_service": data_repo_service},
        config={
            "source_hca_project_id": "abc123",
            "qualifier": "dcp999",
            "managed_access": False,
            "dataset_qualifier": "dcp1"
        }
    )

    snapshot_creation_config = project_snapshot_creation_config(init_context)

    snapshot_creation_time = dataset_snapshot_formatted_date(datetime.now())
    assert snapshot_creation_config.snapshot_name == f"hca_dev_abc123__20210505_dcp1_{snapshot_creation_time}_dcp999"


def test_project_snapshot_creation_config_none_found(monkeypatch):
    monkeypatch.setenv("ENV", "dev")
    data_repo_service = Mock(spec=DataRepoService)
    data_repo_service.find_dataset = Mock(return_value=None)
    init_context = build_init_resource_context(
        resources={"data_repo_service": data_repo_service},
        config={
            "source_hca_project_id": "abc123",
            "qualifier": "dcp999",
            "managed_access": False,
            "dataset_qualifier": "dcp1"
        }
    )

    with pytest.raises(Exception, match="No dataset for project_id"):
        project_snapshot_creation_config(init_context)


def test_project_snapshot_creation_config_no_dataset_qualifier(monkeypatch):
    monkeypatch.setenv("ENV", "dev")
    data_repo_service = Mock(spec=DataRepoService)
    find_dataset_result = TdrDataset(
        dataset_name="hca_dev_abc123__20210505",
        dataset_id="999888",
        project_id="bq_123_999",
        billing_profile_id="fake_billing_profile_id",
        bq_location="us-central1"
    )
    data_repo_service.find_dataset = Mock(return_value=find_dataset_result)
    init_context = build_init_resource_context(
        resources={"data_repo_service": data_repo_service},
        config={
            "source_hca_project_id": "abc123",
            "qualifier": "dcp999",
            "managed_access": False,
        }
    )

    snapshot_creation_config = project_snapshot_creation_config(init_context)

    snapshot_creation_time = dataset_snapshot_formatted_date(datetime.now())
    assert snapshot_creation_config.snapshot_name == f"hca_dev_abc123__20210505_{snapshot_creation_time}_dcp999"


def test_project_snapshot_creation_config_no_snapshot_qualifier(monkeypatch):
    monkeypatch.setenv("ENV", "dev")
    data_repo_service = Mock(spec=DataRepoService)
    find_dataset_result = TdrDataset(
        dataset_name="hca_dev_abc123__20210505",
        dataset_id="999888",
        project_id="bq_123_999",
        billing_profile_id="fake_billing_profile_id",
        bq_location="us-central1"
    )
    data_repo_service.find_dataset = Mock(return_value=find_dataset_result)
    init_context = build_init_resource_context(
        resources={"data_repo_service": data_repo_service},
        config={
            "source_hca_project_id": "abc123",
            "managed_access": False,
        }
    )

    snapshot_creation_config = project_snapshot_creation_config(init_context)

    snapshot_creation_time = dataset_snapshot_formatted_date(datetime.now())
    assert snapshot_creation_config.snapshot_name == f"hca_dev_abc123__20210505_{snapshot_creation_time}"


def test_snapshot_creation_config():
    init_context = build_init_resource_context(
        resources={"run_start_time": ResourceDefinition.hardcoded_resource(1234)},
        config={
            "dataset_name": "hca_prod_20201120_dcp2",
            "managed_access": False,
            "qualifier": "dcp12"
        }
    )

    config = snapshot_creation_config(init_context)
    result = search(LEGACY_SNAPSHOT_NAME_REGEX, config.snapshot_name)
    print(config.snapshot_name)
    assert result, "Snapshot name should pass legacy snapshot regex"
