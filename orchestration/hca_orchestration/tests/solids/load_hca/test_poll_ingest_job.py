import unittest
from unittest.mock import Mock

from dagster import execute_solid, SolidExecutionResult, ResourceDefinition, Failure, ModeDefinition
from data_repo_client import RepositoryApi, ApiException

from hca_manage.common import JobId
from hca_orchestration.solids.load_hca.poll_ingest_job import check_data_ingest_job_result, \
    base_check_data_ingest_job_result


class PollIngestJobTestCase(unittest.TestCase):
    def test_check_data_ingest_job_result_should_poll_jade(self):
        data_repo = Mock(spec=RepositoryApi)
        job_response = {
            "failedFiles": 0
        }
        data_repo.retrieve_job_result = Mock(return_value=job_response)
        mode_def = ModeDefinition(
            name='test',
            resource_defs={
                "data_repo_client": ResourceDefinition.hardcoded_resource(data_repo)
            }
        )

        result: SolidExecutionResult = execute_solid(
            check_data_ingest_job_result,
            mode_def=mode_def,
            input_values={
                'job_id': JobId('fake_job_id')
            }
        )

        self.assertTrue(result.success)

    def test_check_data_ingest_job_result_failed_files(self):
        data_repo = Mock(spec=RepositoryApi)
        job_response = {
            "failedFiles": 1
        }
        data_repo.retrieve_job_result = Mock(return_value=job_response)
        mode_def = ModeDefinition(
            name='test',
            resource_defs={
                "data_repo_client": ResourceDefinition.hardcoded_resource(data_repo)
            }
        )

        with self.assertRaises(
                Failure, msg="Bulk ingest should fail if a file fails to ingest"
        ):
            execute_solid(
                check_data_ingest_job_result,
                mode_def=mode_def,
                input_values={
                    'job_id': JobId('fake_job_id')
                }
            )

    def test_check_data_ingest_job_retries_on_5xx(self):
        data_repo = Mock(spec=RepositoryApi)
        api_responses = [ApiException(status=502), {'failedFiles': 0}]
        data_repo.retrieve_job_result = Mock(side_effect=api_responses)
        mode_def = ModeDefinition(
            name='test',
            resource_defs={
                "data_repo_client": ResourceDefinition.hardcoded_resource(data_repo)
            }
        )

        result: SolidExecutionResult = execute_solid(
            base_check_data_ingest_job_result,
            mode_def=mode_def,
            input_values={
                'job_id': JobId('fake_job_id')
            },
            run_config={
                'solids': {
                    'base_check_data_ingest_job_result': {
                        'config': {
                            'max_wait_time_seconds': 3,
                            'poll_interval_seconds': 1
                        }
                    }
                }
            }
        )

        self.assertTrue(result.success, "Poll ingest should not raise after a single 5xx")
