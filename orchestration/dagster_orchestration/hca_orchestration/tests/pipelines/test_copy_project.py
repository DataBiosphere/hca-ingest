from unittest.mock import MagicMock

from hca_orchestration.pipelines.copy_project import copy_project


def test_copy_project() -> None:
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
