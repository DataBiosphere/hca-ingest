import unittest

from unittest.mock import Mock

from dagster import execute_solid, ModeDefinition, SolidExecutionResult, ResourceDefinition, Failure
from hca_orchestration.config.configurators import preconfigure_resource_for_mode
from hca_orchestration.resources.bigquery import noop_bigquery_client
from hca_orchestration.resources.config.hca_dataset import target_hca_dataset
from hca_orchestration.resources.config.scratch import scratch_config
from hca_orchestration.resources.load_tag import load_tag
from hca_orchestration.resources.storage import local_storage_client
from hca_orchestration.solids.load_hca.load_data_files import diff_file_loads, run_bulk_file_ingest, \
    check_bulk_file_ingest_job_result, JobId
from hca_orchestration.support.typing import HcaScratchDatasetName

from data_repo_client.models import JobModel
from data_repo_client.api import RepositoryApi


class LoadDataFilesTestCase(unittest.TestCase):
    def setUp(self):
        self.load_datafiles_test_mode: ModeDefinition = ModeDefinition(
            name="test",
            resource_defs={
                "storage_client": local_storage_client,
                "bigquery_client": noop_bigquery_client,
                "target_hca_dataset": preconfigure_resource_for_mode(target_hca_dataset, "test"),
                "scratch_config": preconfigure_resource_for_mode(scratch_config, "test"),
                "load_tag": preconfigure_resource_for_mode(load_tag, "test")
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

    def test_run_bulk_file_ingest(self):
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

    def test_check_bulk_file_ingest_job_result(self):
        data_repo = Mock(spec=RepositoryApi)
        job_response = {
            "failedFiles": 0
        }
        data_repo.retrieve_job_result = Mock(return_value=job_response)
        self.load_datafiles_test_mode.resource_defs["data_repo_client"] = ResourceDefinition.hardcoded_resource(
            data_repo)

        result: SolidExecutionResult = execute_solid(
            check_bulk_file_ingest_job_result,
            mode_def=self.load_datafiles_test_mode,
            input_values={
                'job_id': JobId('fake_job_id')
            }
        )

        self.assertTrue(result.success)

    def test_check_bulk_file_ingest_job_result_failed_files(self):
        data_repo = Mock(spec=RepositoryApi)
        job_response = {
            "failedFiles": 1
        }
        data_repo.retrieve_job_result = Mock(return_value=job_response)
        self.load_datafiles_test_mode.resource_defs["data_repo_client"] = ResourceDefinition.hardcoded_resource(
            data_repo)

        with self.assertRaises(
                Failure, msg="Bulk ingest should fail if a file fails to ingest"
        ):
            execute_solid(
                check_bulk_file_ingest_job_result,
                mode_def=self.load_datafiles_test_mode,
                input_values={
                    'job_id': JobId('fake_job_id')
                }
            )
