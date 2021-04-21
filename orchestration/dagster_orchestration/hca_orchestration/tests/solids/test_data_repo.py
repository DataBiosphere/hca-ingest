import unittest
from unittest.mock import Mock, patch

from dagster import execute_solid, Failure, ModeDefinition

from hca_manage.manage import JobId
from hca_orchestration.resources.data_repo import noop_data_repo_client
from hca_orchestration.solids.data_repo import base_wait_for_job_completion


def mock_job_status(completed: bool) -> Mock:
    fake_job_status = Mock()
    fake_job_status.completed = completed

    return fake_job_status


class WaitForJobCompletionTestCase(unittest.TestCase):
    def setUp(self):
        self.test_mode = ModeDefinition(
            name="test",
            resource_defs={
                "data_repo_client": noop_data_repo_client,
            }
        )

    def test_polls_repeatedly_until_complete(self):
        solid_config = {
            "solids": {
                "base_wait_for_job_completion": {
                    "config": {
                        "poll_interval_seconds": 0,
                        "max_wait_time_seconds": 10,
                    }
                }
            }
        }
        job_status_sequence = [
            mock_job_status(False),
            mock_job_status(False),
            mock_job_status(True)
        ]

        with patch('hca_orchestration.resources.data_repo.NoopDataRepoClient.retrieve_job',
                   side_effect=job_status_sequence) as mocked_retrieve_job:
            result = execute_solid(
                base_wait_for_job_completion,
                run_config=solid_config,
                mode_def=self.test_mode,
                input_values={
                    'job_id': JobId('steve-was-here'),
                })
            self.assertTrue(result.success)
            self.assertEqual(mocked_retrieve_job.call_count, 3)

    def test_fails_if_max_time_exceeded(self):
        solid_config = {
            "solids": {
                "base_wait_for_job_completion": {
                    "config": {
                        "poll_interval_seconds": 1,
                        "max_wait_time_seconds": 1,
                    }
                }
            }
        }

        with patch('hca_orchestration.resources.data_repo.NoopDataRepoClient.retrieve_job',
                   return_value=mock_job_status(False)):
            with self.assertRaisesRegex(Failure, "Exceeded max wait time"):
                execute_solid(
                    base_wait_for_job_completion,
                    run_config=solid_config,
                    mode_def=self.test_mode,
                    input_values={
                        'job_id': JobId('steve-was-here'),
                    })
