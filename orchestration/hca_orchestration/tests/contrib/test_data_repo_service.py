from unittest.mock import MagicMock

import pytest
from data_repo_client import RepositoryApi, EnumerateDatasetModel, DatasetSummaryModel, DatasetModel

from hca_orchestration.contrib.data_repo.data_repo_service import DataRepoService, MultipleDatasetException


def test_find_dataset_with_qualifier():
    data_repo_client = MagicMock(spec=RepositoryApi)
    enumerate_datasets_result = EnumerateDatasetModel(
        items=[
            DatasetSummaryModel("dataset_id_1", "hca_dev_12345"),
            DatasetSummaryModel("dataset_id_2", "hca_dev_12345_fake_qualifier"),
            DatasetSummaryModel("dataset_id_3", "hca_dev_fake_qualifier_12345")
        ]
    )
    data_repo_client.enumerate_datasets = MagicMock(return_value=enumerate_datasets_result)
    dataset_storage_info = MagicMock()
    dataset_storage_info.cloud_resource = "bigquery"
    dataset_storage_info.region = "US"
    data_repo_client.retrieve_dataset = MagicMock(return_value=DatasetModel(
        id="dataset_id_2",
        name="hca_dev_12345_fake_qualifier",
        storage=[
            dataset_storage_info
        ]
    )
    )
    data_repo_service = DataRepoService(data_repo_client)

    result = data_repo_service.find_dataset("hca_dev_12345", "fake_qualifier")

    assert result
    assert result.dataset_id == "dataset_id_2"
    data_repo_client.retrieve_dataset.assert_called_once_with(id="dataset_id_2")


def test_find_dataset_no_qualifier():
    data_repo_client = MagicMock(spec=RepositoryApi)
    enumerate_datasets_result = EnumerateDatasetModel(
        items=[
            DatasetSummaryModel("dataset_id_1", "hca_dev_12345"),
        ]
    )
    data_repo_client.enumerate_datasets = MagicMock(return_value=enumerate_datasets_result)
    dataset_storage_info = MagicMock()
    dataset_storage_info.cloud_resource = "bigquery"
    dataset_storage_info.region = "US"
    data_repo_client.retrieve_dataset = MagicMock(return_value=DatasetModel(
        id="dataset_id_1",
        name="hca_dev_12345",
        storage=[
            dataset_storage_info
        ]
    )
    )
    data_repo_service = DataRepoService(data_repo_client)

    result = data_repo_service.find_dataset("hca_dev_12345")

    assert result
    assert result.dataset_id == "dataset_id_1"
    data_repo_client.retrieve_dataset.assert_called_once_with(id="dataset_id_1")


def test_find_dataset_no_matches():
    data_repo_client = MagicMock(spec=RepositoryApi)
    enumerate_datasets_result = EnumerateDatasetModel(
        items=[]
    )
    data_repo_client.enumerate_datasets = MagicMock(return_value=enumerate_datasets_result)

    data_repo_service = DataRepoService(data_repo_client)
    result = data_repo_service.find_dataset("hca_dev_12345", "fake_qualifier")

    assert result is None


def test_find_dataset_exception_multiple_matches():
    data_repo_client = MagicMock(spec=RepositoryApi)
    enumerate_datasets_result = EnumerateDatasetModel(
        items=[
            DatasetSummaryModel("dataset_id_1", "hca_dev_12345"),
            DatasetSummaryModel("dataset_id_2", "hca_dev_12345_fake_qualifier"),
            DatasetSummaryModel("dataset_id_3", "hca_dev_fake_qualifier_12345")
        ]
    )
    data_repo_client.enumerate_datasets = MagicMock(return_value=enumerate_datasets_result)
    data_repo_service = DataRepoService(data_repo_client)

    with pytest.raises(MultipleDatasetException):
        data_repo_service.find_dataset("hca_dev_12345")
