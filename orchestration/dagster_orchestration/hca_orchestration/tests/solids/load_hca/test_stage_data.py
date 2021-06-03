from dagster import SolidExecutionResult, execute_solid

from hca_orchestration.pipelines.load_hca import test_mode
from hca_orchestration.solids.load_hca.stage_data import clear_scratch_dir, pre_process_metadata, create_scratch_dataset

scratch_config = {
    "config": {
        "scratch_bucket_name": "bucket_name",
        "scratch_prefix_name": "prefix_name",
        "scratch_bq_project": "bq_project",
        "scratch_dataset_prefix": "dataset_prefix",
        "scratch_table_expiration_ms": 86400000
    }
}


def test_clear_scratch_dir():
    result: SolidExecutionResult = execute_solid(
        clear_scratch_dir,
        mode_def=test_mode,
        run_config={
            "resources": {
                "scratch_config": scratch_config
            }
        }
    )

    assert result.success


def test_pre_process_metadata():
    result: SolidExecutionResult = execute_solid(
        pre_process_metadata,
        mode_def=test_mode,
        run_config={
            "solids": {
                "pre_process_metadata": {
                    "config": {
                        "input_prefix": "input_prefix"
                    }
                }
            },
            "resources": {
                "scratch_config": scratch_config
            }
        }
    )

    assert result.success


def test_create_scratch_dataset():
    result: SolidExecutionResult = execute_solid(
        create_scratch_dataset,
        mode_def=test_mode,
        run_config={
            "resources": {
                "scratch_config": scratch_config,
                "load_tag": {
                    "config": {
                        "append_timestamp": True,
                        "load_tag_prefix": "load_tag_prefix"
                    }
                }
            }
        }
    )

    assert result.success
