import unittest
from unittest.mock import Mock

from dagster import execute_solid, ModeDefinition, SolidExecutionResult, ResourceDefinition

from dagster_utils.resources.bigquery import noop_bigquery_client
from dagster_utils.resources.google_storage import mock_storage_client
from data_repo_client.api import RepositoryApi
from data_repo_client.models import JobModel
from hca_orchestration.resources.config.hca_dataset import TargetHcaDataset
from hca_orchestration.resources.config.scratch import ScratchConfig
from hca_orchestration.solids.load_hca.data_files.load_data_files import diff_file_loads, run_bulk_file_ingest
from hca_orchestration.support.typing import HcaScratchDatasetName


class LoadDataFilesTestCase(unittest.TestCase):
    def setUp(self):
        self.load_datafiles_test_mode: ModeDefinition = ModeDefinition(
            name="test",
            resource_defs={
                "gcs": mock_storage_client,
                "bigquery_client": noop_bigquery_client,
                "bigquery_service": ResourceDefinition.mock_resource(),
                "target_hca_dataset": ResourceDefinition.hardcoded_resource(
                    TargetHcaDataset(
                        "fake_dataset_name",
                        "1234abc",
                        "fake_hca_project_id",
                        "fake_billing_profile_id"
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

    def test_diff_file_loads(self):
        result: SolidExecutionResult = execute_solid(
            diff_file_loads,
            mode_def=self.load_datafiles_test_mode,
            input_values={
                'scratch_dataset_name': HcaScratchDatasetName("fake_bq_project.testing_dataset_prefix_fake_load_tag")
            }
        )

        self.assertTrue(result.success)
        self.assertEqual(len(result.output_values["control_file_path"]), 10)

    def test_run_bulk_file_ingest_should_return_a_jade_job_id(self):
        job_id = "fake_job_id"
        data_repo = Mock(spec=RepositoryApi)
        job_response = Mock(spec=JobModel)
        job_response.id = job_id
        data_repo.bulk_file_load = Mock(return_value=job_response)
        self.load_datafiles_test_mode.resource_defs["data_repo_client"] = ResourceDefinition.hardcoded_resource(
            data_repo)

        result: SolidExecutionResult = execute_solid(
            run_bulk_file_ingest,
            mode_def=self.load_datafiles_test_mode,
            input_values={
                'control_file_path': HcaScratchDatasetName("gs://fake/example/123.txt")
            }
        )

        self.assertTrue(result.success)
        self.assertEqual(result.output_values["result"], job_id, f"Job ID should be {job_id}")
