from dagster import SolidExecutionResult, execute_solid
from dagster_utils.contrib.data_repo.typing import JobId

from hca_orchestration.pipelines.load_hca import test_mode
from hca_orchestration.solids.load_hca.data_files.load_data_metadata_files import inject_file_ids_solid, \
    file_metadata_fanout
from hca_orchestration.support.typing import HcaScratchDatasetName, MetadataType, MetadataTypeFanoutResult

run_config = {
    "resources": {
        "load_tag": {
            "config": {
                "append_timestamp": False,
                "load_tag_prefix": "load_tag_prefix"
            }
        },
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
                "dataset_name": "dataset_name",
                "dataset_id": "dataset_id",
                "project_id": "project_id",
                "billing_profile_id": "billing_profile_id"
            }
        }
    }
}

metadata_fanout_result = MetadataTypeFanoutResult(
    scratch_dataset_name=HcaScratchDatasetName("dataset"),
    metadata_type=MetadataType("metadata"),
    path="path"
)


def test_ingest_metadata_for_file_type():
    result: SolidExecutionResult = execute_solid(
        inject_file_ids_solid,
        mode_def=test_mode,
        input_values={
            "file_metadata_fanout_result": metadata_fanout_result
        },
        run_config=run_config
    )

    assert result.success


def test_file_metadata_fanout():
    result: SolidExecutionResult = execute_solid(
        file_metadata_fanout,
        mode_def=test_mode,
        input_values={
            "result": [JobId("abcdef")],
            "scratch_dataset_name": HcaScratchDatasetName("dataset")
        },
        run_config=run_config
    )

    assert result.success
