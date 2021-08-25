from unittest.mock import MagicMock

from dagster import ModeDefinition, ResourceDefinition, SolidExecutionResult, execute_solid
from dagster_utils.resources.beam.local_beam_runner import LocalBeamRunner
from google.cloud.bigquery import Client

from hca_orchestration.pipelines.load_hca import test_mode
from hca_orchestration.solids.load_hca.stage_data import clear_scratch_dir, pre_process_metadata, create_scratch_dataset
from hca_orchestration.tests.support.gcs import FakeGCSClient, FakeGoogleBucket, HexBlobInfo


test_bucket = FakeGoogleBucket(
    {"gs://my-fake-bucket/fake-prefix": HexBlobInfo(hex_md5="b2d6ec45472467c836f253bd170182c7", content="test content")}
)

test_bucket_name = "my-fake-bucket"

stage_data_test_mode = ModeDefinition(
    "test_stage_data",
    resource_defs={**test_mode.resource_defs}
)
stage_data_test_mode.resource_defs["gcs"] = ResourceDefinition.hardcoded_resource(
    FakeGCSClient(
        buckets={test_bucket_name: test_bucket}
    )
)

mock_beam_runner = MagicMock(LocalBeamRunner)
stage_data_test_mode.resource_defs["beam_runner"] = ResourceDefinition.hardcoded_resource(mock_beam_runner)

mock_bq_client = MagicMock(Client)
stage_data_test_mode.resource_defs["bigquery_client"] = ResourceDefinition.hardcoded_resource(mock_bq_client)

scratch_config = {
    "config": {
        "scratch_bucket_name": "my-fake-bucket",
        "scratch_bq_project": "bq_project",
        "scratch_dataset_prefix": "dataset_prefix",
        "scratch_table_expiration_ms": 86400000
    }
}


def test_clear_scratch_dir():
    result: SolidExecutionResult = execute_solid(
        clear_scratch_dir,
        mode_def=stage_data_test_mode,
        run_config={
            "resources": {
                "load_tag": {
                    "config": {
                        "append_timestamp": False,
                        "load_tag_prefix": "fake"
                    }
                },
                "scratch_config": scratch_config
            }
        }
    )

    assert result.success
    assert result.output_value() == 1


def test_pre_process_metadata():
    result: SolidExecutionResult = execute_solid(
        pre_process_metadata,
        mode_def=stage_data_test_mode,
        run_config={
            "solids": {
                "pre_process_metadata": {
                    "config": {
                        "input_prefix": "input_prefix"
                    }
                }
            },
            "resources": {
                "scratch_config": scratch_config,
                "load_tag": {
                    "config": {
                        "append_timestamp": False,
                        "load_tag_prefix": "fake"
                    }
                },
            }
        }
    )

    assert result.success
    assert mock_beam_runner.run.called_once()


def test_create_scratch_dataset():
    result: SolidExecutionResult = execute_solid(
        create_scratch_dataset,
        mode_def=test_mode,
        run_config={
            "resources": {
                "scratch_config": scratch_config,
                "load_tag": {
                    "config": {
                        "append_timestamp": False,
                        "load_tag_prefix": "load_tag_prefix"
                    }
                }
            }
        }
    )

    assert result.success
    assert mock_bq_client.create_dataset.called_once()
