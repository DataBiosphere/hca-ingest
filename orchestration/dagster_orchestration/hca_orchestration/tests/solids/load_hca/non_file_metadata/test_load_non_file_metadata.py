from dagster import SolidExecutionResult, execute_solid
from dagster_utils.contrib.data_repo.typing import JobId

from hca_orchestration.pipelines.load_hca import test_mode
from hca_orchestration.solids.load_hca.non_file_metadata.load_non_file_metadata import non_file_metadata_fanout
from hca_orchestration.support.typing import HcaScratchDatasetName


run_config = {
                "resources": {
                    "scratch_config": {
                        "config": {
                            "scratch_bucket_name": "bucket_name",
                            "scratch_prefix_name": "prefix_name",
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


def test_non_file_metadata_fanout():
    result: SolidExecutionResult = execute_solid(
        non_file_metadata_fanout,
        mode_def=test_mode,
        input_values={
            "result": [JobId("abcdef")],
            "scratch_dataset_name": HcaScratchDatasetName("dataset")
        },
        run_config=run_config
    )

    assert result.success
