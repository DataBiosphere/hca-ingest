import pytest
from dagster import SolidExecutionResult, execute_solid, ModeDefinition, ResourceDefinition
from dagster_utils.contrib.data_repo.typing import JobId

from hca_orchestration.solids.load_hca.data_files.load_data_metadata_files import inject_file_ids_solid, \
    file_metadata_fanout
from hca_orchestration.support.typing import HcaScratchDatasetName, MetadataType, MetadataTypeFanoutResult
from hca_orchestration.models.hca_dataset import TdrDataset


@pytest.fixture
def testing_mode_def():
    return ModeDefinition(
        resource_defs={
            "scratch_config": ResourceDefinition.mock_resource(),
            "bigquery_service": ResourceDefinition.mock_resource(),
            "data_repo_client": ResourceDefinition.mock_resource(),
            "target_hca_dataset": ResourceDefinition.hardcoded_resource(TdrDataset("fake", "fake", "fake", "fake", "fake")),
            "data_repo_service": ResourceDefinition.mock_resource(),
            "gcs": ResourceDefinition.mock_resource()
        }
    )


def test_ingest_metadata_for_file_type(testing_mode_def):
    metadata_fanout_result = MetadataTypeFanoutResult(
        scratch_dataset_name=HcaScratchDatasetName("dataset"),
        metadata_type=MetadataType("metadata"),
        path="path"
    )

    result: SolidExecutionResult = execute_solid(
        inject_file_ids_solid,
        mode_def=testing_mode_def,
        input_values={
            "file_metadata_fanout_result": metadata_fanout_result
        },
    )

    assert result.success


def test_file_metadata_fanout(testing_mode_def):
    result: SolidExecutionResult = execute_solid(
        file_metadata_fanout,
        mode_def=testing_mode_def,
        input_values={
            "result": [JobId("abcdef")],
            "scratch_dataset_name": HcaScratchDatasetName("dataset")
        },
    )

    assert result.success
