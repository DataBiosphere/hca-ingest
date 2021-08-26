from unittest.mock import MagicMock, patch, Mock

from hca_orchestration.pipelines.copy_project import copy_project


@patch("hca_manage.bq_managers.DanglingFileRefManager.get_rows", return_value=set())
@patch("hca_manage.bq_managers.NullFileRefManager.get_rows", return_value=set())
@patch("hca_manage.bq_managers.NullFileRefManager.get_file_table_names", return_value=set())
@patch("hca_manage.bq_managers.DuplicatesManager.get_rows", return_value=set())
@patch("hca_manage.bq_managers.DuplicatesManager.get_all_table_names", return_value=set())
def test_copy_project(mock_all_table_names: Mock, mock_duplicates: Mock, mock_file_table_names: Mock,
                      mock_null_filerefs: Mock, mock_dangling_proj_refs: Mock) -> None:
    result = copy_project.execute_in_process(
        resources={
            "bigquery_client": MagicMock(),
            "data_repo_client": MagicMock(),
            "gcs": MagicMock(),
            "scratch_config": MagicMock(),
            "bigquery_service": MagicMock(),
            "hca_project_copying_config": MagicMock(),
            "target_hca_dataset": MagicMock(),
            "load_tag": MagicMock(),
        })
    assert result.success
