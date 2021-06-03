from dagster import SolidExecutionResult, execute_solid
from dagster_utils.contrib.data_repo.typing import JobId

from hca_orchestration.pipelines.load_hca import test_mode
from hca_orchestration.solids.load_hca.load_table import check_has_data, check_has_outdated, clear_outdated, \
    load_table, start_load
from hca_orchestration.support.typing import HcaScratchDatasetName, MetadataType, MetadataTypeFanoutResult


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
metadata_fanout_result = MetadataTypeFanoutResult(
    scratch_dataset_name=HcaScratchDatasetName("dataset"),
    metadata_type=MetadataType("metadata"),
    path="path"
)


def test_load_table():
    result: SolidExecutionResult = execute_solid(
        load_table,
        mode_def=test_mode,
        input_values={
            "metadata_fanout_result": metadata_fanout_result
        },
        run_config=run_config
    )

    assert result.success


def test_clear_outdated():
    result: SolidExecutionResult = execute_solid(
        clear_outdated,
        mode_def=test_mode,
        input_values={
            "has_outdated": True,
            "metadata_fanout_result": metadata_fanout_result
        },
        run_config=run_config
    )

    assert result.success
    assert result.output_value() == "abcdef"


# TODO test_check_has_data_false
def test_check_has_data_true():
    result: SolidExecutionResult = execute_solid(
        check_has_data,
        mode_def=test_mode,
        input_values={
            "metadata_fanout_result": metadata_fanout_result
        },
        run_config=run_config
    )

    assert result.success
    assert result.output_value("has_data")


# TODO test_check_has_outdated_false
def test_check_has_outdated_true():
    result: SolidExecutionResult = execute_solid(
        check_has_outdated,
        mode_def=test_mode,
        input_values={
            "parent_load_job_id": JobId("abcdef"),
            "metadata_fanout_result": metadata_fanout_result
        },
        run_config=run_config
    )

    assert result.success
    assert result.output_value("has_outdated")


# TODO test_start_load_yes_new_rows
def test_start_load_no_new_rows():
    result: SolidExecutionResult = execute_solid(
        start_load,
        mode_def=test_mode,
        input_values={
            "has_data": True,
            "metadata_fanout_result": metadata_fanout_result
        },
        run_config=run_config
    )

    assert result.success
    assert result.output_value("no_job") == "no_job"
