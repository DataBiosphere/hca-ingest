from unittest.mock import Mock

from hca_orchestration.contrib.bigquery import BigQueryService
from hca_orchestration.models.entities import MetadataEntity
from hca_orchestration.models.hca_dataset import TdrDataset
from hca_orchestration.solids.copy_project.subgraph_hydration import (
    _extract_entities_to_path,
    _fetch_file_entities,
    _find_previously_loaded_target_paths,
)
from hca_orchestration.support.typing import MetadataType


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


def test_extract_entities_to_path():
    bq_service = Mock(spec=BigQueryService)
    nodes = {
        MetadataType("fake_metadata_type_1"): [
            MetadataEntity(MetadataType("fake_metadata_type_1"), "fake_id_1"),
            MetadataEntity(MetadataType("fake_metadata_type_1"), "fake_id_2"),
            MetadataEntity(MetadataType("fake_metadata_type_1"), "fake_id_3"),
            MetadataEntity(MetadataType("fake_metadata_type_1"), "fake_id_4")

        ],
        MetadataType("fake_metadata_type_2"): [
            MetadataEntity(MetadataType("fake_metadata_type_2"), "fake_id_4"),
            MetadataEntity(MetadataType("fake_metadata_type_2"), "fake_id_5"),
            MetadataEntity(MetadataType("fake_metadata_type_2"), "fake_id_6"),
            MetadataEntity(MetadataType("fake_metadata_type_2"), "fake_id_7")
        ]
    }

    _extract_entities_to_path(
        nodes,
        "gs://example/foo",
        "fake_project_id",
        "fake_snapshot_name",
        "us-fake-1",
        bq_service,
        2
    )

    assert bq_service.run_query.call_count == 4
    query_params = [call.args[-1][0].values for call in bq_service.run_query.call_args_list]

    assert query_params[0] == ['fake_id_1', 'fake_id_2']
    assert query_params[1] == ['fake_id_3', 'fake_id_4']
    assert query_params[2] == ['fake_id_4', 'fake_id_5']
    assert query_params[3] == ['fake_id_6', 'fake_id_7']


def test_fetch_file_entities():
    entities_by_type = {
        MetadataType("sequence_file"): [
            MetadataEntity(MetadataType("sequence_file"), "abc"),
            MetadataEntity(MetadataType("sequence_file"), "cde"),
            MetadataEntity(MetadataType("sequence_file"), "efg"),
            MetadataEntity(MetadataType("sequence_file"), "ghi")
        ],
        MetadataType("analysis_file"): [
            MetadataEntity(MetadataType("analysis_file"), "ijk"),
            MetadataEntity(MetadataType("analysis_file"), "klm"),
            MetadataEntity(MetadataType("analysis_file"), "mno"),
            MetadataEntity(MetadataType("analysis_file"), "pqr")
        ]
    }
    bq_service = Mock(spec=BigQueryService)
    results = [
        [{"sequence_file_id": "chunk_1"}],
        [{"sequence_file_id": "chunk_2"}],
        [{"analysis_file_id": "chunk_2"}],
        [{"analysis_file_id": "chunk_3"}]
    ]
    bq_service.run_query = Mock(side_effect=results)

    result = _fetch_file_entities(
        entities_by_type,
        "fake_project_id",
        "fake_snapshot_name",
        "us-fake-1",
        bq_service,
        2
    )

    assert bq_service.run_query.call_count == 4
    assert result[MetadataType("sequence_file")] == [
        {"sequence_file_id": "chunk_1"},
        {"sequence_file_id": "chunk_2"},
    ]
    assert result[MetadataType("analysis_file")] == [
        {"analysis_file_id": "chunk_2"},
        {"analysis_file_id": "chunk_3"}
    ]
