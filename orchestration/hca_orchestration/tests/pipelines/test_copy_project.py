from unittest.mock import MagicMock, patch, Mock

from dagster import ResourceDefinition

from hca_orchestration.contrib.bigquery import BigQueryService
from hca_orchestration.pipelines.copy_project import copy_project
from hca_orchestration.models.hca_dataset import TdrDataset
from hca_orchestration.resources.hca_project_config import HcaProjectCopyingConfig


@patch("hca_manage.bq_managers.DanglingFileRefManager.get_rows", return_value=set())
@patch("hca_manage.bq_managers.NullFileRefManager.get_rows", return_value=set())
@patch("hca_manage.bq_managers.NullFileRefManager.get_file_table_names", return_value=set())
@patch("hca_manage.bq_managers.DuplicatesManager.get_rows", return_value=set())
@patch("hca_manage.bq_managers.DuplicatesManager.get_all_table_names", return_value=set())
@patch("hca_manage.bq_managers.CountsManager.get_rows", return_value=set())
def test_copy_project(*mocks) -> None:
    bq_service = MagicMock(spec=BigQueryService)
    bq_service.get_projects_in_dataset = MagicMock(return_value={"fake_source_project_id"})
    bq_service.get_links_in_dataset = MagicMock(return_value=[])
    bq_service.bigquery_client = MagicMock()
    result = copy_project.execute_in_process(
        resources={
            "bigquery_client": MagicMock(),
            "data_repo_client": MagicMock(),
            "gcs": MagicMock(),
            "scratch_config": MagicMock(),
            "bigquery_service": ResourceDefinition.hardcoded_resource(bq_service),
            "hca_project_copying_config": HcaProjectCopyingConfig("fake_source_project_id", "fake_source_snapshot_name", "fake_bq_project_id", "fake_region"),
            "target_hca_dataset": TdrDataset("fake_name", "fake_id", "fake_gcp_project_id", "fake_billing_profile_id", "us-fake-region"),
            "load_tag": MagicMock(),
        })
    assert result.success
