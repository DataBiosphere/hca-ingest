from unittest.mock import Mock

import pytest
from dagster import resource, ModeDefinition, ResourceDefinition, SolidExecutionResult, execute_solid
from dagster_utils.contrib.data_repo.typing import JobId
from google.cloud.storage import Client, Blob

from hca_orchestration.contrib.bigquery import BigQueryService
from hca_orchestration.contrib.data_repo.data_repo_service import DataRepoService
from hca_orchestration.models.hca_dataset import TdrDataset
from hca_orchestration.models.scratch import ScratchConfig
from hca_orchestration.pipelines.load_hca import test_mode
from hca_orchestration.solids.load_hca.load_table import load_table_solid, clear_outdated
from hca_orchestration.support.typing import HcaScratchDatasetName, MetadataType, MetadataTypeFanoutResult
from hca_orchestration.tests.support.gcs import FakeGCSClient, FakeGoogleBucket, HexBlobInfo


@pytest.fixture
def test_bucket_name():
    return "my-fake-bucket"


@pytest.fixture
def run_config(test_bucket_name):
    return {
        "resources": {
            "load_tag": {
                "config": {
                    "load_tag_prefix": "fake",
                    "append_run_id": False
                }
            },
            "scratch_config": {
                "config": {
                    "scratch_bucket_name": test_bucket_name,
                    "scratch_bq_project": "bq_project",
                    "scratch_dataset_prefix": "dataset_prefix",
                    "scratch_table_expiration_ms": 86400000
                }
            },
            "target_hca_dataset": {
                "config": {
                    "dataset_id": "dataset_id"
                }
            }
        }
    }


@pytest.fixture
def metadata_fanout_result():
    return MetadataTypeFanoutResult(
        scratch_dataset_name=HcaScratchDatasetName("dataset"),
        metadata_type=MetadataType("metadata"),
        path="path"
    )


@pytest.fixture
def data_repo_service():
    data_repo_service = Mock(spec=DataRepoService)
    data_repo_service.ingest_data = Mock(return_value=JobId("fake_ingest_job_id"))
    data_repo_service.delete_data = Mock(return_value=JobId("fake_delete_job_id"))
    return data_repo_service


@pytest.fixture
def load_table_test_mode(data_repo_service, test_bucket_name):
    base_def = ModeDefinition(
        "test_load_table",
        resource_defs={
            **test_mode.resource_defs,
        }
    )

    test_bucket = FakeGoogleBucket(
        {"gs://my-fake-bucket/fake-prefix": HexBlobInfo(
            hex_md5="b2d6ec45472467c836f253bd170182c7", content="test content")}
    )
    base_def.resource_defs["gcs"] = ResourceDefinition.hardcoded_resource(
        FakeGCSClient(
            buckets={test_bucket_name: test_bucket}
        )
    )

    base_def.resource_defs["data_repo_service"] = ResourceDefinition.hardcoded_resource(
        data_repo_service
    )

    return base_def


def test_load_table(
        load_table_test_mode,
        metadata_fanout_result,
        run_config
):
    result: SolidExecutionResult = execute_solid(
        load_table_solid,
        mode_def=load_table_test_mode,
        input_values={
            "metadata_fanout_result": metadata_fanout_result
        },
        run_config=run_config
    )

    assert result.success


def test_clear_outdated(data_repo_service):
    scratch_config = ScratchConfig(
        "fake_scratch_bucket",
        "fake_scratch_prefix",
        "fake_bq_project",
        "fake_scratch_dataset_prefix",
        0
    )
    target_hca_dataset = TdrDataset(
        "fake_target_dataset_name",
        "1234abc",
        "fake_target_bq_project_id",
        "fake_billing_profile_id",
        "fake_location"
    )

    gcs = Mock(spec=Client)
    blob = Mock(spec=Blob)
    blob.size = 1
    gcs.list_blobs = Mock(return_value=[blob])

    job_id = clear_outdated(
        scratch_config,
        target_hca_dataset,
        MetadataType("sequence_file"),
        Mock(spec=BigQueryService),
        data_repo_service,
        gcs
    )

    assert job_id == "fake_delete_job_id"


def test_load_table_yes_new_rows(
        load_table_test_mode,
        metadata_fanout_result,
        run_config
):
    @resource
    def _mock_bq_service(_init_context) -> BigQueryService:
        svc = Mock(spec=BigQueryService)
        svc.get_num_rows_in_table = Mock(return_value=1)
        return svc

    this_test_mode = ModeDefinition(
        "test_start_load",
        resource_defs={**load_table_test_mode.resource_defs}
    )
    this_test_mode.resource_defs["bigquery_service"] = _mock_bq_service

    result: SolidExecutionResult = execute_solid(
        load_table_solid,
        mode_def=this_test_mode,
        input_values={
            "metadata_fanout_result": metadata_fanout_result
        },
        run_config=run_config
    )

    assert result.success
    assert result.output_value("result") == "fake_delete_job_id"


def test_load_table_no_new_rows(
        load_table_test_mode,
        metadata_fanout_result,
        run_config
):
    result: SolidExecutionResult = execute_solid(
        load_table_solid,
        mode_def=load_table_test_mode,
        input_values={
            "metadata_fanout_result": metadata_fanout_result
        },
        run_config=run_config
    )

    assert result.success
    assert result.output_value("result") is None
