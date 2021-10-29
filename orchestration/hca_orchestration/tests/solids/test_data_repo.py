import unittest
from unittest.mock import Mock, patch

from dagster import execute_solid, Failure, ModeDefinition, ResourceDefinition
from data_repo_client import RepositoryApi

from hca_manage.common import JobId
from hca_orchestration.solids.data_repo import base_wait_for_job_completion


def mock_job_status(completed: bool, successful: bool = True) -> Mock:
    fake_job_status = Mock()
    if completed:
        fake_job_status.completed = "2001-01-01 00:00:00"
        fake_job_status.job_status = "succeeded" if successful else "failed"
    else:
        fake_job_status.completed = None
        fake_job_status.job_status = "running"

    return fake_job_status


data_repo = Mock(spec=RepositoryApi)


class WaitForJobCompletionTestCase(unittest.TestCase):
    def setUp(self):
        self.test_mode = ModeDefinition(
            name="test",
            resource_defs={
                "data_repo_client": ResourceDefinition.hardcoded_resource(data_repo)
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

        data_repo.retrieve_job = Mock(side_effect=job_status_sequence)
        self.test_mode.resource_defs["data_repo_client"] = ResourceDefinition.hardcoded_resource(data_repo)

        result = execute_solid(
            base_wait_for_job_completion,
            run_config=solid_config,
            mode_def=self.test_mode,
            input_values={
                'job_id': JobId('steve-was-here'),
            })
        self.assertTrue(result.success)
        self.assertEqual(data_repo.retrieve_job.call_count, 3)

    def test_fails_if_job_failed(self):
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

        data_repo.retrieve_job = Mock(return_value=mock_job_status(completed=True, successful=False))
        self.test_mode.resource_defs["data_repo_client"] = ResourceDefinition.hardcoded_resource(data_repo)

        with self.assertRaisesRegex(Failure, "Job did not complete successfully."):
            result = execute_solid(
                base_wait_for_job_completion,
                run_config=solid_config,
                mode_def=self.test_mode,
                input_values={
                    'job_id': JobId('steve-was-here'),
                })

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

        data_repo.retrieve_job = Mock(return_value=mock_job_status(False))
        self.test_mode.resource_defs["data_repo_client"] = ResourceDefinition.hardcoded_resource(data_repo)

        with self.assertRaisesRegex(Failure, "Exceeded max wait time"):
            execute_solid(
                base_wait_for_job_completion,
                run_config=solid_config,
                mode_def=self.test_mode,
                input_values={
                    'job_id': JobId('steve-was-here'),
                })
