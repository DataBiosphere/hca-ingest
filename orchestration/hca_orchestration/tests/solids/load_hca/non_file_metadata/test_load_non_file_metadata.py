from dagster import SolidExecutionResult, execute_solid, ModeDefinition, ResourceDefinition
from dagster_utils.contrib.data_repo.typing import JobId

from hca_orchestration.solids.load_hca.non_file_metadata.load_non_file_metadata import non_file_metadata_fanout
from hca_orchestration.support.typing import HcaScratchDatasetName
from hca_orchestration.models.hca_dataset import TdrDataset

run_config = {
    "resources": {
        "scratch_config": {
            "config": {
                "scratch_bucket_name": "bucket_name",
                "scratch_bq_project": "bq_project",
                "scratch_dataset_prefix": "dataset_prefix",
                "scratch_table_expiration_ms": 86400000
            }
        },
        "target_hca_dataset": {
            "config": {
                "dataset_id": "dataset_id",
            }
        }
    }
}


def test_non_file_metadata_fanout():
    target_dataset = TdrDataset("fake", "fake", "fake", "fake", "fake")
    result: SolidExecutionResult = execute_solid(
        non_file_metadata_fanout,
        mode_def=ModeDefinition(resource_defs={
            "scratch_config": ResourceDefinition.mock_resource(),
            "gcs": ResourceDefinition.mock_resource(),
            "target_hca_dataset": ResourceDefinition.hardcoded_resource(target_dataset),
            "bigquery_service": ResourceDefinition.mock_resource(),
            "data_repo_service": ResourceDefinition.mock_resource()
        }),
        input_values={
            "result": [JobId("abcdef")],
            "scratch_dataset_name": HcaScratchDatasetName("dataset")
        },
        run_config=run_config
    )

    assert result.success
