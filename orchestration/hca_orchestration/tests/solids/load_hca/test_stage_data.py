from unittest.mock import MagicMock

import pytest
from dagster import ModeDefinition, ResourceDefinition, SolidExecutionResult, execute_solid, Failure
from dagster_utils.resources.beam.local_beam_runner import LocalBeamRunner
from google.cloud.bigquery import Client

from hca_orchestration.solids.load_hca.stage_data import clear_scratch_dir, pre_process_metadata, create_scratch_dataset
from hca_orchestration.tests.support.gcs import FakeGCSClient, FakeGoogleBucket, HexBlobInfo
from hca_orchestration.models.scratch import ScratchConfig
from hca_orchestration.models.hca_dataset import TdrDataset


@pytest.fixture
def beam_runner():
    return MagicMock(LocalBeamRunner)


@pytest.fixture
def bigquery_client():
    return MagicMock(Client)


@pytest.fixture
def testing_mode_def(beam_runner, bigquery_client):
    scratch_config = ScratchConfig(
        scratch_bucket_name="fake-bucket",
        scratch_prefix_name="fake-prefix",
        scratch_bq_project="bq_project",
        scratch_dataset_prefix="dataset_prefix",
        scratch_table_expiration_ms=86400000
    )
    test_bucket = FakeGoogleBucket(
        {f"gs://{scratch_config.scratch_bucket_name}/{scratch_config.scratch_prefix_name}":
            HexBlobInfo(hex_md5="b2d6ec45472467c836f253bd170182c7", content="test content")}
    )

    return ModeDefinition(
        resource_defs={
            "beam_runner": ResourceDefinition.hardcoded_resource(beam_runner),
            "gcs": ResourceDefinition.hardcoded_resource(
                FakeGCSClient(
                    buckets={scratch_config.scratch_bucket_name: test_bucket}
                )
            ),
            "scratch_config": ResourceDefinition.hardcoded_resource(scratch_config),
            "bigquery_client": ResourceDefinition.hardcoded_resource(bigquery_client),
            "load_tag": ResourceDefinition.hardcoded_resource("fake_load_tag"),
            "target_hca_dataset": ResourceDefinition.hardcoded_resource(TdrDataset("fake", "fake", "fake", "fake", "fake"))
        }
    )


def test_clear_scratch_dir(testing_mode_def):
    result: SolidExecutionResult = execute_solid(
        clear_scratch_dir,
        mode_def=testing_mode_def
    )

    assert result.success
    assert result.output_value() == 1


def test_pre_process_metadata(testing_mode_def, beam_runner):
    result: SolidExecutionResult = execute_solid(
        pre_process_metadata,
        mode_def=testing_mode_def,
        run_config={
            "solids": {
                "pre_process_metadata": {
                    "config": {
                        "input_prefix": "gs://foobar/example"
                    }
                }
            }
        }
    )

    assert result.success
    assert beam_runner.run.called_once()


def test_pre_process_metadata_fails_with_non_gs_path(testing_mode_def):
    with pytest.raises(Failure, match="must start with gs"):
        execute_solid(
            pre_process_metadata,
            mode_def=testing_mode_def,
            run_config={
                "solids": {
                    "pre_process_metadata": {
                        "config": {
                            "input_prefix": "input_prefix"
                        }
                    }
                }
            }
        )


def test_pre_process_metadata_fails_with_trailing_slash(testing_mode_def):
    with pytest.raises(Failure, match="must not end with trailing slash"):
        execute_solid(
            pre_process_metadata,
            mode_def=testing_mode_def,
            run_config={
                "solids": {
                    "pre_process_metadata": {
                        "config": {
                            "input_prefix": "gs://example/"
                        }
                    }
                }
            }
        )


def test_create_scratch_dataset(testing_mode_def, bigquery_client):
    result: SolidExecutionResult = execute_solid(
        create_scratch_dataset,
        mode_def=testing_mode_def,

    )

    assert result.success
    assert bigquery_client.create_dataset.called_once()
