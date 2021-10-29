from unittest.mock import Mock, MagicMock

from dagster import execute_solid, ModeDefinition, SolidExecutionResult, ResourceDefinition, resource

from data_repo_client.api import RepositoryApi
from data_repo_client.models import JobModel
from hca_orchestration.models.hca_dataset import TdrDataset
from hca_orchestration.resources.config.scratch import ScratchConfig
from hca_orchestration.solids.load_hca.data_files.load_data_files import diff_file_loads, run_bulk_file_ingest
from hca_orchestration.support.typing import HcaScratchDatasetName

from google.cloud.storage import Client, Blob


@resource
def _gcs(_init_context) -> Client:
    gcs = MagicMock(spec=Client)
    gcs.list_blobs = MagicMock(
        return_value=[
            Blob(name='fake_blob_0', bucket='fake_bucket'),
            Blob(name='fake_blob_1', bucket='fake_bucket'),
            Blob(name='fake_blob_2', bucket='fake_bucket'),
            Blob(name='fake_blob_3', bucket='fake_bucket'),
            Blob(name='fake_blob_4', bucket='fake_bucket'),
            Blob(name='fake_blob_5', bucket='fake_bucket'),
            Blob(name='fake_blob_6', bucket='fake_bucket'),
            Blob(name='fake_blob_7', bucket='fake_bucket'),
            Blob(name='fake_blob_8', bucket='fake_bucket'),
            Blob(name='fake_blob_9', bucket='fake_bucket')]
    )
    return gcs


load_datafiles_test_mode: ModeDefinition = ModeDefinition(
    name="test",
    resource_defs={
        "gcs": _gcs,
        "bigquery_client": ResourceDefinition.mock_resource(),
        "bigquery_service": ResourceDefinition.mock_resource(),
        "target_hca_dataset": ResourceDefinition.hardcoded_resource(
            TdrDataset(
                "fake_dataset_name",
                "1234abc",
                "fake_hca_project_id",
                "fake_billing_profile_id",
                "us-fake-region"
            )),
        "scratch_config": ResourceDefinition.hardcoded_resource(
            ScratchConfig(
                "fake_bucket",
                "fake_prefix",
                "fake_bq_project",
                "fake_dataset_prefix",
                1
            )
        ),
        "load_tag": ResourceDefinition.hardcoded_resource("fake_load_tag")
    }
)


def test_diff_file_loads():
    result: SolidExecutionResult = execute_solid(
        diff_file_loads,
        mode_def=load_datafiles_test_mode,
        input_values={
            'scratch_dataset_name': HcaScratchDatasetName("fake_bq_project.testing_dataset_prefix_fake_load_tag")
        }
    )

    assert result.success
    assert len(result.output_values["control_file_path"]) == 10


def test_run_bulk_file_ingest_should_return_a_jade_job_id():
    job_id = "fake_job_id"
    data_repo = Mock(spec=RepositoryApi)
    job_response = Mock(spec=JobModel)
    job_response.id = job_id
    data_repo.bulk_file_load = Mock(return_value=job_response)
    load_datafiles_test_mode.resource_defs["data_repo_client"] = ResourceDefinition.hardcoded_resource(
        data_repo)

    result: SolidExecutionResult = execute_solid(
        run_bulk_file_ingest,
        mode_def=load_datafiles_test_mode,
        input_values={
            'control_file_path': HcaScratchDatasetName("gs://fake/example/123.txt")
        }
    )

    assert result.success
    assert result.output_values["result"] == job_id, f"Job ID should be {job_id}"
