from dagster import resource, InitResourceContext
from unittest.mock import MagicMock

from hca_orchestration.contrib.data_repo.data_repo_service import DataRepoService
from hca_orchestration.models.hca_dataset import TdrDataset


@resource(
    required_resource_keys={"data_repo_client"}
)
def data_repo_service(init_context: InitResourceContext) -> DataRepoService:
    return DataRepoService(init_context.resources.data_repo_client)


@resource
def mock_data_repo_service(init_context: InitResourceContext) -> DataRepoService:
    mock_svc = MagicMock()
    mock_svc.get_dataset = MagicMock(
        return_value=TdrDataset(
            "fake_dataset",
            "fake_dataset_id",
            "fake_project_id",
            "fake_billing_profile_id",
            "fake_bq_location"))
    return mock_svc
