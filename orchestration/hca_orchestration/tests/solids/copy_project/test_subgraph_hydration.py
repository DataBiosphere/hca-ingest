from unittest.mock import Mock

from hca_orchestration.contrib.bigquery import BigQueryService
from hca_orchestration.models.hca_dataset import TdrDataset
from hca_orchestration.solids.copy_project.subgraph_hydration import _find_previously_loaded_target_paths


def test_find_previously_loaded_target_paths():
    target_paths = {"a/b/c", "c/d/e", "e/f/g", "g/h/i"}
    target_dataset = TdrDataset(
        "fake_dataset_name",
        "fake_dataset_id",
        "fake_project_id",
        "fake_profile_id",
        "us-fake-1"
    )
    bq_service = Mock(spec=BigQueryService)
    results = [[{"target_path": "abc"}], [{"target_path": "bcd"}]]
    bq_service.run_query = Mock(side_effect=results)

    loaded = _find_previously_loaded_target_paths(
        target_paths,
        target_dataset,
        bq_service,
        2
    )

    assert loaded == {"abc", "bcd"}
    assert bq_service.run_query.call_count == 2
